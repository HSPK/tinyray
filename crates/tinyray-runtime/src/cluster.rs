//! Cluster state: which nodes exist, what they have, and where actors live.
//!
//! With no stateless tasks there is no high-frequency scheduling to do, so the
//! head is a single-threaded piece of bookkeeping. It participates when an
//! actor is created, looked up, or dies -- never on the data path.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tinyray_core::{ActorId, NodeId};

/// Resources a node offers or an actor consumes.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Resources {
    pub num_cpus: f64,
    /// Whole GPUs. Fractions are allowed for hyperparameter trials, but a
    /// collective group requires at least one whole GPU per member.
    pub num_gpus: f64,
    pub memory_bytes: u64,
    #[serde(default)]
    pub custom: HashMap<String, f64>,
}

impl Resources {
    pub fn cpus(num_cpus: f64) -> Resources {
        Resources {
            num_cpus,
            ..Default::default()
        }
    }

    /// Whether `self` can cover `demand`.
    pub fn covers(&self, demand: &Resources) -> bool {
        if self.num_cpus + f64::EPSILON < demand.num_cpus {
            return false;
        }
        if self.num_gpus + f64::EPSILON < demand.num_gpus {
            return false;
        }
        if self.memory_bytes < demand.memory_bytes {
            return false;
        }
        demand.custom.iter().all(|(name, needed)| {
            self.custom.get(name).copied().unwrap_or(0.0) + f64::EPSILON >= *needed
        })
    }

    pub fn subtract(&mut self, demand: &Resources) {
        self.num_cpus -= demand.num_cpus;
        self.num_gpus -= demand.num_gpus;
        self.memory_bytes = self.memory_bytes.saturating_sub(demand.memory_bytes);
        for (name, needed) in &demand.custom {
            *self.custom.entry(name.clone()).or_insert(0.0) -= needed;
        }
    }

    /// Cap every field at `total`, so a double release cannot create capacity.
    pub fn clamp_to(&mut self, total: &Resources) {
        self.num_cpus = self.num_cpus.min(total.num_cpus);
        self.num_gpus = self.num_gpus.min(total.num_gpus);
        self.memory_bytes = self.memory_bytes.min(total.memory_bytes);
        for (name, amount) in self.custom.iter_mut() {
            if let Some(cap) = total.custom.get(name) {
                *amount = amount.min(*cap);
            }
        }
    }

    pub fn add(&mut self, released: &Resources) {
        self.num_cpus += released.num_cpus;
        self.num_gpus += released.num_gpus;
        self.memory_bytes += released.memory_bytes;
        for (name, amount) in &released.custom {
            *self.custom.entry(name.clone()).or_insert(0.0) += amount;
        }
    }
}

/// A machine in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: NodeId,
    /// `host:port` of the node agent.
    pub endpoint: String,
    pub hostname: String,
    pub total: Resources,
    pub available: Resources,
    /// Physical GPU indices not currently assigned to an actor.
    pub free_gpus: Vec<u32>,
}

/// Where an actor is and whether it is usable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorInfo {
    pub actor_id: ActorId,
    pub name: Option<String>,
    pub node_id: NodeId,
    /// `host:port` of the actor's own HTTP server.
    pub endpoint: String,
    pub state: ActorState,
    pub resources: Resources,
    pub gpu_ids: Vec<u32>,
    pub restarts: u32,
    pub max_restarts: u32,
    /// Detached actors outlive the driver that created them.
    pub detached: bool,
}

/// Lifecycle of an actor, as the head sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActorState {
    /// Placed, process starting.
    Starting,
    Alive,
    /// Died and is eligible for another attempt.
    Restarting,
    /// Gone for good.
    Dead,
}

/// Why a placement request could not be satisfied.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlacementError {
    #[error("no node can satisfy the request: {detail}")]
    Infeasible { detail: String },
    #[error(
        "cluster has {available} of {requested} bundles free; gang placement is all or nothing"
    )]
    GangUnsatisfiable { requested: usize, available: usize },
    #[error("no nodes are registered")]
    EmptyCluster,
}

/// How to spread a group of actors across the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Strategy {
    /// Fill one node before moving on. Good for fractional-GPU trials, where
    /// packing keeps fragmentation down.
    Pack,
    /// Spread across nodes. Good for rollouts, which want independent CPUs and
    /// network interfaces.
    Spread,
    /// Spread, and fail rather than co-locate two members.
    StrictSpread,
}

/// The head's view of the world.
pub struct ClusterState {
    nodes: HashMap<NodeId, NodeInfo>,
    last_heartbeat: HashMap<NodeId, Instant>,
    actors: HashMap<ActorId, ActorInfo>,
    names: HashMap<String, ActorId>,
    heartbeat_timeout: Duration,
}

impl ClusterState {
    pub fn new(heartbeat_timeout: Duration) -> ClusterState {
        ClusterState {
            nodes: HashMap::new(),
            last_heartbeat: HashMap::new(),
            actors: HashMap::new(),
            names: HashMap::new(),
            heartbeat_timeout,
        }
    }

    pub fn register_node(&mut self, info: NodeInfo) {
        self.last_heartbeat.insert(info.node_id, Instant::now());
        self.nodes.insert(info.node_id, info);
    }

    pub fn heartbeat(
        &mut self,
        node_id: NodeId,
        available: Resources,
        free_gpus: Vec<u32>,
    ) -> bool {
        let Some(node) = self.nodes.get_mut(&node_id) else {
            return false;
        };
        node.available = available;
        node.free_gpus = free_gpus;
        self.last_heartbeat.insert(node_id, Instant::now());
        true
    }

    /// Nodes that have missed their heartbeat deadline.
    pub fn dead_nodes(&self) -> Vec<NodeId> {
        let now = Instant::now();
        self.last_heartbeat
            .iter()
            .filter(|(_, last)| now.duration_since(**last) > self.heartbeat_timeout)
            .map(|(node_id, _)| *node_id)
            .collect()
    }

    /// Remove a node and report the actors that died with it.
    pub fn remove_node(&mut self, node_id: NodeId) -> Vec<ActorId> {
        self.nodes.remove(&node_id);
        self.last_heartbeat.remove(&node_id);
        let orphaned: Vec<ActorId> = self
            .actors
            .values()
            .filter(|actor| actor.node_id == node_id && actor.state != ActorState::Dead)
            .map(|actor| actor.actor_id)
            .collect();
        for actor_id in &orphaned {
            if let Some(actor) = self.actors.get_mut(actor_id) {
                actor.state = ActorState::Dead;
            }
        }
        orphaned
    }

    pub fn nodes(&self) -> Vec<NodeInfo> {
        self.nodes.values().cloned().collect()
    }

    pub fn node(&self, node_id: NodeId) -> Option<&NodeInfo> {
        self.nodes.get(&node_id)
    }

    pub fn actors(&self) -> Vec<ActorInfo> {
        self.actors.values().cloned().collect()
    }

    pub fn actor(&self, actor_id: ActorId) -> Option<&ActorInfo> {
        self.actors.get(&actor_id)
    }

    pub fn actor_by_name(&self, name: &str) -> Option<&ActorInfo> {
        self.names.get(name).and_then(|id| self.actors.get(id))
    }

    /// Reserve resources for one actor.
    pub fn place(
        &mut self,
        resources: &Resources,
        strategy: Strategy,
        exclude: &[NodeId],
    ) -> Result<(NodeId, Vec<u32>), PlacementError> {
        if self.nodes.is_empty() {
            return Err(PlacementError::EmptyCluster);
        }

        let needed_gpus = whole_gpus(resources.num_gpus);
        let mut candidates: Vec<&NodeInfo> = self
            .nodes
            .values()
            .filter(|node| !exclude.contains(&node.node_id))
            .filter(|node| node.available.covers(resources))
            .filter(|node| node.free_gpus.len() >= needed_gpus)
            .collect();

        if candidates.is_empty() {
            return Err(PlacementError::Infeasible {
                detail: describe_shortfall(&self.nodes, resources, needed_gpus),
            });
        }

        // Pack prefers the busiest node that still fits; spread prefers the
        // emptiest. Both are stable, so placement does not depend on hash order.
        match strategy {
            Strategy::Pack => candidates.sort_by(|a, b| {
                a.available
                    .num_cpus
                    .partial_cmp(&b.available.num_cpus)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.node_id.cmp(&b.node_id))
            }),
            Strategy::Spread | Strategy::StrictSpread => candidates.sort_by(|a, b| {
                b.available
                    .num_cpus
                    .partial_cmp(&a.available.num_cpus)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.node_id.cmp(&b.node_id))
            }),
        }

        let node_id = candidates[0].node_id;
        let node = self.nodes.get_mut(&node_id).expect("candidate exists");
        node.available.subtract(resources);
        let gpu_ids: Vec<u32> = node.free_gpus.drain(..needed_gpus).collect();
        Ok((node_id, gpu_ids))
    }

    /// Reserve resources for `count` actors, all or nothing.
    ///
    /// Gang placement is a requirement rather than an optimisation: 32 rollout
    /// actors that start halfway cannot form a collective group, and the run
    /// deadlocks waiting for members that will never arrive.
    pub fn place_gang(
        &mut self,
        resources: &Resources,
        count: usize,
        strategy: Strategy,
    ) -> Result<Vec<(NodeId, Vec<u32>)>, PlacementError> {
        let capacity = self.gang_capacity(resources, strategy);
        if capacity < count {
            return Err(PlacementError::GangUnsatisfiable {
                requested: count,
                available: capacity,
            });
        }

        let mut placements = Vec::with_capacity(count);
        let mut used_nodes = Vec::new();
        for _ in 0..count {
            let exclude = if strategy == Strategy::StrictSpread {
                used_nodes.clone()
            } else {
                Vec::new()
            };
            match self.place(resources, strategy, &exclude) {
                Ok(placement) => {
                    used_nodes.push(placement.0);
                    placements.push(placement);
                }
                Err(err) => {
                    // Roll back so a failed gang leaves no resources stranded.
                    for (node_id, gpu_ids) in placements {
                        self.release(node_id, resources, &gpu_ids);
                    }
                    return Err(err);
                }
            }
        }
        Ok(placements)
    }

    /// How many actors of this shape the cluster could currently host.
    pub fn gang_capacity(&self, resources: &Resources, strategy: Strategy) -> usize {
        let needed_gpus = whole_gpus(resources.num_gpus);
        if strategy == Strategy::StrictSpread {
            return self
                .nodes
                .values()
                .filter(|node| {
                    node.available.covers(resources) && node.free_gpus.len() >= needed_gpus
                })
                .count();
        }
        self.nodes
            .values()
            .map(|node| fits_per_node(node, resources, needed_gpus))
            .sum()
    }

    /// Return resources to a node.
    ///
    /// Clamped to the node's total. A double release is a bug in the caller,
    /// but left unchecked it does not merely lose track of a GPU -- it invents
    /// one, and the scheduler then places two processes on hardware that fits
    /// one. That failure surfaces much later, somewhere else, and usually
    /// looks like a NCCL problem.
    pub fn release(&mut self, node_id: NodeId, resources: &Resources, gpu_ids: &[u32]) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.available.add(resources);
            node.available.clamp_to(&node.total);
            node.free_gpus.extend_from_slice(gpu_ids);
            node.free_gpus.sort_unstable();
            node.free_gpus.dedup();
            // Physical devices cannot exceed what the node reported either.
            node.free_gpus
                .retain(|gpu| (*gpu as f64) < node.total.num_gpus);
        }
    }

    pub fn add_actor(&mut self, info: ActorInfo) {
        if let Some(name) = &info.name {
            self.names.insert(name.clone(), info.actor_id);
        }
        self.actors.insert(info.actor_id, info);
    }

    pub fn set_actor_state(&mut self, actor_id: ActorId, state: ActorState) {
        if let Some(actor) = self.actors.get_mut(&actor_id) {
            actor.state = state;
        }
    }

    pub fn set_actor_endpoint(&mut self, actor_id: ActorId, endpoint: String) {
        if let Some(actor) = self.actors.get_mut(&actor_id) {
            actor.endpoint = endpoint;
        }
    }

    /// Record a death and say whether the actor should be restarted.
    pub fn note_actor_died(&mut self, actor_id: ActorId) -> bool {
        let Some(actor) = self.actors.get_mut(&actor_id) else {
            return false;
        };
        if actor.restarts < actor.max_restarts {
            actor.restarts += 1;
            actor.state = ActorState::Restarting;
            true
        } else {
            actor.state = ActorState::Dead;
            false
        }
    }

    /// Forget an actor and hand its resources back.
    pub fn remove_actor(&mut self, actor_id: ActorId) -> Option<ActorInfo> {
        let actor = self.actors.remove(&actor_id)?;
        if let Some(name) = &actor.name {
            self.names.remove(name);
        }
        // Released immediately rather than at the next heartbeat: a
        // hyperparameter sweep starts and stops actors constantly, and waiting
        // for a heartbeat would idle the cluster.
        let resources = actor.resources.clone();
        let gpu_ids = actor.gpu_ids.clone();
        let node_id = actor.node_id;
        self.release(node_id, &resources, &gpu_ids);
        Some(actor)
    }
}

fn whole_gpus(num_gpus: f64) -> usize {
    // Fractional requests share a GPU and so reserve none exclusively.
    if num_gpus >= 1.0 {
        num_gpus.floor() as usize
    } else {
        0
    }
}

fn fits_per_node(node: &NodeInfo, resources: &Resources, needed_gpus: usize) -> usize {
    let mut fits = usize::MAX;
    if resources.num_cpus > 0.0 {
        fits = fits.min((node.available.num_cpus / resources.num_cpus).floor() as usize);
    }
    if let Some(per_node) = node.free_gpus.len().checked_div(needed_gpus) {
        fits = fits.min(per_node);
    } else if resources.num_gpus > 0.0 {
        fits = fits.min((node.available.num_gpus / resources.num_gpus).floor() as usize);
    }
    if let Some(per_node) = node
        .available
        .memory_bytes
        .checked_div(resources.memory_bytes)
    {
        fits = fits.min(per_node as usize);
    }
    for (name, needed) in &resources.custom {
        if *needed > 0.0 {
            let have = node.available.custom.get(name).copied().unwrap_or(0.0);
            fits = fits.min((have / needed).floor() as usize);
        }
    }
    if fits == usize::MAX {
        // A request for nothing at all still occupies a slot conceptually;
        // treat it as unbounded but cap it so callers get a sane number.
        return 1024;
    }
    fits
}

/// Explain *why* nothing fits. A bare "infeasible" is useless when 32 actors
/// fail to start at 3am.
fn describe_shortfall(
    nodes: &HashMap<NodeId, NodeInfo>,
    resources: &Resources,
    needed_gpus: usize,
) -> String {
    let best_cpus = nodes
        .values()
        .map(|n| n.available.num_cpus)
        .fold(f64::NEG_INFINITY, f64::max);
    let best_gpus = nodes.values().map(|n| n.free_gpus.len()).max().unwrap_or(0);
    format!(
        "requested {:.2} CPUs and {} whole GPUs; the best node has {:.2} CPUs and {} free GPUs across {} node(s)",
        resources.num_cpus,
        needed_gpus,
        if best_cpus.is_finite() { best_cpus } else { 0.0 },
        best_gpus,
        nodes.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u64, cpus: f64, gpus: usize) -> NodeInfo {
        let resources = Resources {
            num_cpus: cpus,
            num_gpus: gpus as f64,
            memory_bytes: 64 << 30,
            custom: HashMap::new(),
        };
        NodeInfo {
            node_id: NodeId::from_parts(0, id),
            endpoint: format!("10.0.0.{id}:6380"),
            hostname: format!("node{id}"),
            total: resources.clone(),
            available: resources,
            free_gpus: (0..gpus as u32).collect(),
        }
    }

    fn cluster(nodes: Vec<NodeInfo>) -> ClusterState {
        let mut state = ClusterState::new(Duration::from_secs(30));
        for node in nodes {
            state.register_node(node);
        }
        state
    }

    fn gpu_actor() -> Resources {
        Resources {
            num_cpus: 4.0,
            num_gpus: 1.0,
            ..Default::default()
        }
    }

    #[test]
    fn placement_on_an_empty_cluster_is_an_error_not_a_panic() {
        let mut state = ClusterState::new(Duration::from_secs(30));
        assert_eq!(
            state.place(&Resources::cpus(1.0), Strategy::Spread, &[]),
            Err(PlacementError::EmptyCluster)
        );
    }

    #[test]
    fn place_reserves_cpus_and_gpus() {
        let mut state = cluster(vec![node(1, 16.0, 4)]);
        let (node_id, gpus) = state.place(&gpu_actor(), Strategy::Spread, &[]).unwrap();
        assert_eq!(gpus.len(), 1);
        let node = state.node(node_id).unwrap();
        assert_eq!(node.available.num_cpus, 12.0);
        assert_eq!(node.free_gpus.len(), 3);
    }

    #[test]
    fn spread_prefers_the_emptiest_node() {
        let mut state = cluster(vec![node(1, 16.0, 4), node(2, 16.0, 4)]);
        let (first, _) = state.place(&gpu_actor(), Strategy::Spread, &[]).unwrap();
        let (second, _) = state.place(&gpu_actor(), Strategy::Spread, &[]).unwrap();
        assert_ne!(first, second, "spread must not stack onto one node");
    }

    #[test]
    fn pack_fills_one_node_first() {
        let mut state = cluster(vec![node(1, 16.0, 4), node(2, 16.0, 4)]);
        let (first, _) = state.place(&gpu_actor(), Strategy::Pack, &[]).unwrap();
        let (second, _) = state.place(&gpu_actor(), Strategy::Pack, &[]).unwrap();
        assert_eq!(
            first, second,
            "pack should reuse the busiest node that fits"
        );
    }

    #[test]
    fn gpus_are_handed_out_exclusively() {
        let mut state = cluster(vec![node(1, 64.0, 2)]);
        let (_, a) = state.place(&gpu_actor(), Strategy::Pack, &[]).unwrap();
        let (_, b) = state.place(&gpu_actor(), Strategy::Pack, &[]).unwrap();
        assert_ne!(a, b, "two actors were given the same physical GPU");
        // NCCL deadlocks if two ranks share a device, so this must be exact.
        assert!(state.place(&gpu_actor(), Strategy::Pack, &[]).is_err());
    }

    #[test]
    fn fractional_gpus_do_not_reserve_a_device() {
        // Hyperparameter trials share a card; they must not consume the
        // exclusive slots that collective members need.
        let mut state = cluster(vec![node(1, 64.0, 1)]);
        let trial = Resources {
            num_cpus: 1.0,
            num_gpus: 0.25,
            ..Default::default()
        };
        for _ in 0..4 {
            let (_, gpus) = state.place(&trial, Strategy::Pack, &[]).unwrap();
            assert!(gpus.is_empty());
        }
        assert_eq!(
            state
                .node(NodeId::from_parts(0, 1))
                .unwrap()
                .free_gpus
                .len(),
            1
        );
    }

    #[test]
    fn infeasible_placement_explains_itself() {
        let mut state = cluster(vec![node(1, 2.0, 0)]);
        let err = state
            .place(
                &Resources {
                    num_cpus: 4.0,
                    num_gpus: 1.0,
                    ..Default::default()
                },
                Strategy::Spread,
                &[],
            )
            .unwrap_err();
        let message = err.to_string();
        // "Infeasible" alone is useless at 3am; the numbers must be there.
        assert!(
            message.contains("requested 4.00 CPUs and 1 whole GPUs"),
            "{message}"
        );
        assert!(message.contains("free GPUs"), "{message}");
    }

    #[test]
    fn gang_placement_is_all_or_nothing() {
        let mut state = cluster(vec![node(1, 16.0, 2), node(2, 16.0, 2)]);
        // Four fit; eight do not, and the failure must leave nothing reserved.
        let err = state
            .place_gang(&gpu_actor(), 8, Strategy::Spread)
            .unwrap_err();
        assert_eq!(
            err,
            PlacementError::GangUnsatisfiable {
                requested: 8,
                available: 4
            }
        );
        assert_eq!(
            state
                .node(NodeId::from_parts(0, 1))
                .unwrap()
                .free_gpus
                .len(),
            2
        );
        assert_eq!(
            state
                .node(NodeId::from_parts(0, 2))
                .unwrap()
                .free_gpus
                .len(),
            2
        );
    }

    #[test]
    fn gang_placement_succeeds_when_it_exactly_fits() {
        let mut state = cluster(vec![node(1, 16.0, 2), node(2, 16.0, 2)]);
        let placements = state.place_gang(&gpu_actor(), 4, Strategy::Spread).unwrap();
        assert_eq!(placements.len(), 4);
        let all_gpus: Vec<(NodeId, u32)> = placements
            .iter()
            .flat_map(|(node, gpus)| gpus.iter().map(move |g| (*node, *g)))
            .collect();
        let unique: std::collections::HashSet<_> = all_gpus.iter().collect();
        assert_eq!(unique.len(), 4, "a GPU was handed out twice");
    }

    #[test]
    fn strict_spread_refuses_to_co_locate() {
        let mut state = cluster(vec![node(1, 64.0, 4), node(2, 64.0, 4)]);
        assert!(state
            .place_gang(&gpu_actor(), 3, Strategy::StrictSpread)
            .is_err());
        let placements = state
            .place_gang(&gpu_actor(), 2, Strategy::StrictSpread)
            .unwrap();
        assert_ne!(placements[0].0, placements[1].0);
    }

    #[test]
    fn a_double_release_cannot_invent_capacity() {
        // Releasing twice is a caller bug, but the consequence must be a
        // no-op rather than a scheduler that believes in extra hardware.
        let mut state = cluster(vec![node(1, 16.0, 2)]);
        let (node_id, gpu_ids) = state.place(&gpu_actor(), Strategy::Pack, &[]).unwrap();
        state.release(node_id, &gpu_actor(), &gpu_ids);
        state.release(node_id, &gpu_actor(), &gpu_ids);

        let node = state.node(node_id).unwrap();
        assert_eq!(
            node.available.num_cpus, 16.0,
            "CPUs exceeded the node total"
        );
        assert_eq!(node.free_gpus.len(), 2, "a GPU was conjured out of nothing");
    }

    #[test]
    fn releasing_an_unknown_gpu_id_is_ignored() {
        let mut state = cluster(vec![node(1, 16.0, 2)]);
        let node_id = NodeId::from_parts(0, 1);
        state.release(node_id, &Resources::cpus(0.0), &[7, 8, 9]);
        assert_eq!(state.node(node_id).unwrap().free_gpus, vec![0, 1]);
    }

    #[test]
    fn removing_an_actor_returns_its_resources_immediately() {
        // A sweep starts and stops actors constantly; waiting for a heartbeat
        // to reclaim resources would idle the cluster.
        let mut state = cluster(vec![node(1, 16.0, 2)]);
        let (node_id, gpu_ids) = state.place(&gpu_actor(), Strategy::Pack, &[]).unwrap();
        let actor_id = ActorId::generate();
        state.add_actor(ActorInfo {
            actor_id,
            name: None,
            node_id,
            endpoint: "127.0.0.1:1".into(),
            state: ActorState::Alive,
            resources: gpu_actor(),
            gpu_ids: gpu_ids.clone(),
            restarts: 0,
            max_restarts: 0,
            detached: false,
        });

        state.remove_actor(actor_id);
        let node = state.node(node_id).unwrap();
        assert_eq!(node.available.num_cpus, 16.0);
        assert_eq!(node.free_gpus.len(), 2);
    }

    #[test]
    fn named_actors_are_resolvable() {
        let mut state = cluster(vec![node(1, 16.0, 0)]);
        let actor_id = ActorId::generate();
        state.add_actor(ActorInfo {
            actor_id,
            name: Some("ps".into()),
            node_id: NodeId::from_parts(0, 1),
            endpoint: "127.0.0.1:1".into(),
            state: ActorState::Alive,
            resources: Resources::cpus(1.0),
            gpu_ids: vec![],
            restarts: 0,
            max_restarts: 0,
            detached: true,
        });
        assert_eq!(state.actor_by_name("ps").unwrap().actor_id, actor_id);
        state.remove_actor(actor_id);
        assert!(state.actor_by_name("ps").is_none());
    }

    #[test]
    fn restart_budget_is_respected() {
        let mut state = cluster(vec![node(1, 16.0, 0)]);
        let actor_id = ActorId::generate();
        state.add_actor(ActorInfo {
            actor_id,
            name: None,
            node_id: NodeId::from_parts(0, 1),
            endpoint: "127.0.0.1:1".into(),
            state: ActorState::Alive,
            resources: Resources::cpus(1.0),
            gpu_ids: vec![],
            restarts: 0,
            max_restarts: 2,
            detached: false,
        });

        assert!(state.note_actor_died(actor_id));
        assert!(state.note_actor_died(actor_id));
        assert!(!state.note_actor_died(actor_id), "budget must be finite");
        assert_eq!(state.actor(actor_id).unwrap().state, ActorState::Dead);
    }

    #[test]
    fn a_dead_node_takes_its_actors_with_it() {
        let mut state = cluster(vec![node(1, 16.0, 0), node(2, 16.0, 0)]);
        let doomed = ActorId::generate();
        let survivor = ActorId::generate();
        for (actor_id, node_index) in [(doomed, 1u64), (survivor, 2)] {
            state.add_actor(ActorInfo {
                actor_id,
                name: None,
                node_id: NodeId::from_parts(0, node_index),
                endpoint: "127.0.0.1:1".into(),
                state: ActorState::Alive,
                resources: Resources::cpus(1.0),
                gpu_ids: vec![],
                restarts: 0,
                max_restarts: 0,
                detached: false,
            });
        }

        let orphaned = state.remove_node(NodeId::from_parts(0, 1));
        assert_eq!(orphaned, vec![doomed]);
        assert_eq!(state.actor(doomed).unwrap().state, ActorState::Dead);
        assert_eq!(state.actor(survivor).unwrap().state, ActorState::Alive);
    }

    #[test]
    fn heartbeats_keep_a_node_alive() {
        let mut state = ClusterState::new(Duration::from_millis(50));
        state.register_node(node(1, 16.0, 0));
        assert!(state.dead_nodes().is_empty());
        std::thread::sleep(Duration::from_millis(80));
        assert_eq!(state.dead_nodes(), vec![NodeId::from_parts(0, 1)]);

        state.heartbeat(NodeId::from_parts(0, 1), Resources::cpus(16.0), vec![]);
        assert!(state.dead_nodes().is_empty());
    }

    #[test]
    fn heartbeat_from_an_unknown_node_is_rejected() {
        let mut state = ClusterState::new(Duration::from_secs(30));
        assert!(!state.heartbeat(NodeId::generate(), Resources::cpus(1.0), vec![]));
    }

    #[test]
    fn resources_cover_comparison_handles_custom_types() {
        let mut have = Resources::cpus(8.0);
        have.custom.insert("accelerator".into(), 2.0);
        let mut want = Resources::cpus(4.0);
        want.custom.insert("accelerator".into(), 1.0);
        assert!(have.covers(&want));
        want.custom.insert("accelerator".into(), 3.0);
        assert!(!have.covers(&want));
        want.custom.insert("accelerator".into(), 1.0);
        want.custom.insert("unobtainium".into(), 1.0);
        assert!(!have.covers(&want));
    }
}

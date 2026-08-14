//! Collective group bookkeeping.
//!
//! tinyray implements **no collective transport at all**. Weight broadcast goes
//! through NCCL via `torch.distributed`, and this module supplies the one thing
//! NCCL does not: who is rank what, where they meet, and what happens when the
//! membership changes.
//!
//! The hard part is the last one. A NCCL communicator is not fault tolerant:
//! when any rank dies, every collective on that communicator hangs forever. So
//! a group carries an **epoch**, and any membership change bumps it, aborts the
//! old communicator and rebuilds. Group creation costs seconds, which is why
//! groups must be long-lived and never rebuilt per iteration.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tinyray_core::ActorId;

/// State of a collective group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupState {
    /// Members have been told their ranks and are calling `init_process_group`.
    Forming,
    /// Every member acknowledged; collectives may run.
    Ready,
    /// A member died or timed out. Every collective must fail fast until the
    /// group is rebuilt, rather than hang inside NCCL.
    Broken,
    /// Torn down for good.
    Destroyed,
}

/// Why a group could not be created.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum AdmissionError {
    #[error("collective groups need at least two members, got {0}")]
    TooSmall(usize),

    #[error(
        "actor {actor} requests {num_gpus} GPUs; collective members must own at least one whole \
         GPU because NCCL is GPU-only and two ranks sharing a device deadlock"
    )]
    NotWholeGpu { actor: ActorId, num_gpus: f64 },

    #[error("actor {0} appears twice in the member list")]
    DuplicateMember(ActorId),

    #[error("actor {0} is not alive")]
    MemberNotAlive(ActorId),

    #[error("two members are on node {node} sharing GPU {gpu}; NCCL would deadlock")]
    SharedDevice { node: String, gpu: u32 },
}

/// A member's place in a group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub actor_id: ActorId,
    pub rank: usize,
    /// Node the actor runs on, used to detect two ranks on one device.
    pub node_id: String,
    /// Physical GPUs the actor owns exclusively.
    pub gpu_ids: Vec<u32>,
}

/// What a member needs in order to join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rendezvous {
    pub group_id: String,
    pub epoch: u64,
    pub rank: usize,
    pub world_size: usize,
    /// Host of the `TCPStore` that rank 0 runs.
    pub store_host: String,
    pub store_port: u16,
    pub backend: String,
}

/// One collective group.
#[derive(Debug, Clone)]
pub struct Group {
    pub group_id: String,
    pub epoch: u64,
    pub state: GroupState,
    pub members: Vec<Member>,
    pub backend: String,
    pub store_host: String,
    pub store_port: u16,
    /// Members that have acknowledged the current epoch.
    acknowledged: Vec<ActorId>,
}

impl Group {
    pub fn world_size(&self) -> usize {
        self.members.len()
    }

    pub fn rank_of(&self, actor_id: ActorId) -> Option<usize> {
        self.members
            .iter()
            .find(|m| m.actor_id == actor_id)
            .map(|m| m.rank)
    }

    pub fn contains(&self, actor_id: ActorId) -> bool {
        self.members.iter().any(|m| m.actor_id == actor_id)
    }

    /// The rendezvous information for one member of the current epoch.
    pub fn rendezvous_for(&self, actor_id: ActorId) -> Option<Rendezvous> {
        let rank = self.rank_of(actor_id)?;
        Some(Rendezvous {
            group_id: self.group_id.clone(),
            epoch: self.epoch,
            rank,
            world_size: self.world_size(),
            store_host: self.store_host.clone(),
            store_port: self.store_port,
            backend: self.backend.clone(),
        })
    }

    pub fn acknowledged(&self) -> usize {
        self.acknowledged.len()
    }

    pub fn is_ready(&self) -> bool {
        self.state == GroupState::Ready
    }
}

/// All groups the head knows about.
#[derive(Default)]
pub struct CollectiveRegistry {
    groups: HashMap<String, Group>,
}

impl CollectiveRegistry {
    pub fn new() -> CollectiveRegistry {
        CollectiveRegistry::default()
    }

    /// Validate a proposed membership and assign ranks.
    ///
    /// Every rule enforced here exists because breaking it produces a hang
    /// rather than an error: NCCL is GPU-only, and two ranks sharing a device
    /// deadlock instead of failing.
    #[allow(clippy::type_complexity)]
    pub fn create(
        &mut self,
        group_id: String,
        candidates: Vec<(ActorId, f64, String, Vec<u32>, bool)>,
        backend: String,
        store_host: String,
        store_port: u16,
    ) -> Result<Group, AdmissionError> {
        if candidates.len() < 2 {
            return Err(AdmissionError::TooSmall(candidates.len()));
        }

        let mut seen = Vec::new();
        let mut devices: Vec<(String, u32)> = Vec::new();
        let mut members = Vec::with_capacity(candidates.len());

        for (rank, (actor_id, num_gpus, node_id, gpu_ids, alive)) in
            candidates.into_iter().enumerate()
        {
            if seen.contains(&actor_id) {
                return Err(AdmissionError::DuplicateMember(actor_id));
            }
            seen.push(actor_id);

            if !alive {
                return Err(AdmissionError::MemberNotAlive(actor_id));
            }
            if num_gpus < 1.0 || gpu_ids.is_empty() {
                return Err(AdmissionError::NotWholeGpu {
                    actor: actor_id,
                    num_gpus,
                });
            }
            for gpu in &gpu_ids {
                let key = (node_id.clone(), *gpu);
                if devices.contains(&key) {
                    return Err(AdmissionError::SharedDevice {
                        node: node_id.clone(),
                        gpu: *gpu,
                    });
                }
                devices.push(key);
            }

            members.push(Member {
                actor_id,
                rank,
                node_id,
                gpu_ids,
            });
        }

        let group = Group {
            group_id: group_id.clone(),
            epoch: 0,
            state: GroupState::Forming,
            members,
            backend,
            store_host,
            store_port,
            acknowledged: Vec::new(),
        };
        self.groups.insert(group_id, group.clone());
        Ok(group)
    }

    pub fn get(&self, group_id: &str) -> Option<&Group> {
        self.groups.get(group_id)
    }

    /// Record that a member finished `init_process_group` for this epoch.
    pub fn acknowledge(
        &mut self,
        group_id: &str,
        actor_id: ActorId,
        epoch: u64,
    ) -> Option<GroupState> {
        let group = self.groups.get_mut(group_id)?;
        // A late ack from a previous epoch is stale and must be ignored, or a
        // rebuilt group would look ready before its members had rejoined.
        if epoch != group.epoch || !group.contains(actor_id) {
            return Some(group.state);
        }
        if !group.acknowledged.contains(&actor_id) {
            group.acknowledged.push(actor_id);
        }
        if group.acknowledged.len() == group.members.len() && group.state == GroupState::Forming {
            group.state = GroupState::Ready;
        }
        Some(group.state)
    }

    /// Mark a group unusable. Returns the members that must abort their
    /// communicator.
    ///
    /// Fast failure matters here: a surviving rank that enters a collective on
    /// a dead communicator blocks forever inside NCCL.
    pub fn break_group(&mut self, group_id: &str, _reason: &str) -> Vec<ActorId> {
        let Some(group) = self.groups.get_mut(group_id) else {
            return Vec::new();
        };
        if group.state == GroupState::Destroyed {
            return Vec::new();
        }
        group.state = GroupState::Broken;
        group.acknowledged.clear();
        group.members.iter().map(|m| m.actor_id).collect()
    }

    /// Every group an actor belongs to. Used when that actor dies.
    pub fn groups_with(&self, actor_id: ActorId) -> Vec<String> {
        self.groups
            .values()
            .filter(|group| group.contains(actor_id) && group.state != GroupState::Destroyed)
            .map(|group| group.group_id.clone())
            .collect()
    }

    /// Bump the epoch and start forming again.
    ///
    /// Rebuilding takes seconds, so groups are meant to be long-lived. Doing
    /// this once per training iteration would dominate the run.
    pub fn begin_rebuild(&mut self, group_id: &str) -> Option<Group> {
        let group = self.groups.get_mut(group_id)?;
        if group.state == GroupState::Destroyed {
            return None;
        }
        group.epoch += 1;
        group.state = GroupState::Forming;
        group.acknowledged.clear();
        Some(group.clone())
    }

    /// Replace a member, keeping rank assignments stable for everyone else.
    pub fn replace_member(
        &mut self,
        group_id: &str,
        old: ActorId,
        new: ActorId,
        node_id: String,
        gpu_ids: Vec<u32>,
    ) -> bool {
        let Some(group) = self.groups.get_mut(group_id) else {
            return false;
        };
        let Some(member) = group.members.iter_mut().find(|m| m.actor_id == old) else {
            return false;
        };
        member.actor_id = new;
        member.node_id = node_id;
        member.gpu_ids = gpu_ids;
        group.acknowledged.retain(|id| *id != old);
        true
    }

    pub fn destroy(&mut self, group_id: &str) -> Vec<ActorId> {
        let Some(group) = self.groups.get_mut(group_id) else {
            return Vec::new();
        };
        group.state = GroupState::Destroyed;
        group.members.iter().map(|m| m.actor_id).collect()
    }

    pub fn group_ids(&self) -> Vec<String> {
        self.groups.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(index: u64, node: &str, gpu: u32) -> (ActorId, f64, String, Vec<u32>, bool) {
        (
            ActorId::from_parts(0, index),
            1.0,
            node.to_string(),
            vec![gpu],
            true,
        )
    }

    fn registry_with_group(size: u64) -> (CollectiveRegistry, Group) {
        let mut registry = CollectiveRegistry::new();
        let candidates: Vec<_> = (0..size)
            .map(|i| candidate(i, &format!("node{}", i % 2), (i / 2) as u32))
            .collect();
        let group = registry
            .create(
                "g1".into(),
                candidates,
                "nccl".into(),
                "10.0.0.1".into(),
                29500,
            )
            .expect("valid group");
        (registry, group)
    }

    #[test]
    fn ranks_are_assigned_in_order() {
        let (_, group) = registry_with_group(4);
        assert_eq!(group.world_size(), 4);
        for index in 0..4u64 {
            assert_eq!(
                group.rank_of(ActorId::from_parts(0, index)),
                Some(index as usize)
            );
        }
    }

    #[test]
    fn rendezvous_carries_everything_a_member_needs() {
        let (_, group) = registry_with_group(4);
        let rendezvous = group.rendezvous_for(ActorId::from_parts(0, 2)).unwrap();
        assert_eq!(rendezvous.rank, 2);
        assert_eq!(rendezvous.world_size, 4);
        assert_eq!(rendezvous.store_host, "10.0.0.1");
        assert_eq!(rendezvous.store_port, 29500);
        assert_eq!(rendezvous.backend, "nccl");
        assert_eq!(rendezvous.epoch, 0);
    }

    #[test]
    fn a_group_needs_at_least_two_members() {
        let mut registry = CollectiveRegistry::new();
        let err = registry
            .create(
                "g".into(),
                vec![candidate(0, "n0", 0)],
                "nccl".into(),
                "h".into(),
                1,
            )
            .unwrap_err();
        assert_eq!(err, AdmissionError::TooSmall(1));
    }

    #[test]
    fn fractional_gpu_members_are_refused_with_an_explanation() {
        // The rule that decides whether a deployment can use NCCL at all.
        let mut registry = CollectiveRegistry::new();
        let err = registry
            .create(
                "g".into(),
                vec![
                    candidate(0, "n0", 0),
                    (ActorId::from_parts(0, 1), 0.25, "n0".into(), vec![], true),
                ],
                "nccl".into(),
                "h".into(),
                1,
            )
            .unwrap_err();
        let message = err.to_string();
        assert!(message.contains("at least one whole GPU"), "{message}");
        assert!(message.contains("deadlock"), "{message}");
    }

    #[test]
    fn two_ranks_on_one_device_are_refused() {
        // NCCL deadlocks rather than erroring, so this must be caught here.
        let mut registry = CollectiveRegistry::new();
        let err = registry
            .create(
                "g".into(),
                vec![candidate(0, "n0", 0), candidate(1, "n0", 0)],
                "nccl".into(),
                "h".into(),
                1,
            )
            .unwrap_err();
        assert!(matches!(err, AdmissionError::SharedDevice { gpu: 0, .. }));
    }

    #[test]
    fn duplicate_members_are_refused() {
        let mut registry = CollectiveRegistry::new();
        let err = registry
            .create(
                "g".into(),
                vec![candidate(0, "n0", 0), candidate(0, "n1", 1)],
                "nccl".into(),
                "h".into(),
                1,
            )
            .unwrap_err();
        assert!(matches!(err, AdmissionError::DuplicateMember(_)));
    }

    #[test]
    fn dead_members_are_refused() {
        let mut registry = CollectiveRegistry::new();
        let err = registry
            .create(
                "g".into(),
                vec![
                    candidate(0, "n0", 0),
                    (ActorId::from_parts(0, 1), 1.0, "n1".into(), vec![0], false),
                ],
                "nccl".into(),
                "h".into(),
                1,
            )
            .unwrap_err();
        assert!(matches!(err, AdmissionError::MemberNotAlive(_)));
    }

    #[test]
    fn group_becomes_ready_only_when_everyone_acknowledges() {
        let (mut registry, group) = registry_with_group(4);
        for index in 0..3u64 {
            let state = registry
                .acknowledge("g1", ActorId::from_parts(0, index), group.epoch)
                .unwrap();
            assert_eq!(state, GroupState::Forming);
        }
        let state = registry
            .acknowledge("g1", ActorId::from_parts(0, 3), group.epoch)
            .unwrap();
        assert_eq!(state, GroupState::Ready);
    }

    #[test]
    fn duplicate_acknowledgements_do_not_fake_readiness() {
        let (mut registry, group) = registry_with_group(4);
        for _ in 0..10 {
            registry.acknowledge("g1", ActorId::from_parts(0, 0), group.epoch);
        }
        assert_eq!(registry.get("g1").unwrap().state, GroupState::Forming);
        assert_eq!(registry.get("g1").unwrap().acknowledged(), 1);
    }

    #[test]
    fn stale_acknowledgements_are_ignored() {
        // A slow member acking the previous epoch must not make a rebuilding
        // group look ready before everyone has actually rejoined.
        let (mut registry, _) = registry_with_group(2);
        registry.acknowledge("g1", ActorId::from_parts(0, 0), 0);
        registry.acknowledge("g1", ActorId::from_parts(0, 1), 0);
        assert_eq!(registry.get("g1").unwrap().state, GroupState::Ready);

        registry.break_group("g1", "a rank died");
        registry.begin_rebuild("g1");
        assert_eq!(registry.get("g1").unwrap().epoch, 1);

        registry.acknowledge("g1", ActorId::from_parts(0, 0), 0); // stale
        assert_eq!(registry.get("g1").unwrap().acknowledged(), 0);
        assert_eq!(registry.get("g1").unwrap().state, GroupState::Forming);
    }

    #[test]
    fn breaking_a_group_names_everyone_who_must_abort() {
        let (mut registry, _) = registry_with_group(4);
        let to_abort = registry.break_group("g1", "rank 2 died");
        assert_eq!(
            to_abort.len(),
            4,
            "every rank must abort, not just the dead one"
        );
        assert_eq!(registry.get("g1").unwrap().state, GroupState::Broken);
    }

    #[test]
    fn rebuilding_bumps_the_epoch_and_clears_acknowledgements() {
        let (mut registry, group) = registry_with_group(2);
        registry.acknowledge("g1", ActorId::from_parts(0, 0), group.epoch);
        registry.break_group("g1", "restart");

        let rebuilt = registry.begin_rebuild("g1").unwrap();
        assert_eq!(rebuilt.epoch, 1);
        assert_eq!(rebuilt.state, GroupState::Forming);
        assert_eq!(rebuilt.acknowledged(), 0);
        // Members must be told the new epoch, or they would rejoin the old one.
        assert_eq!(
            rebuilt
                .rendezvous_for(ActorId::from_parts(0, 0))
                .unwrap()
                .epoch,
            1
        );
    }

    #[test]
    fn replacing_a_member_keeps_other_ranks_stable() {
        let (mut registry, _) = registry_with_group(4);
        let replacement = ActorId::generate();
        assert!(registry.replace_member(
            "g1",
            ActorId::from_parts(0, 2),
            replacement,
            "node9".into(),
            vec![3]
        ));
        let group = registry.get("g1").unwrap();
        assert_eq!(group.rank_of(replacement), Some(2));
        assert_eq!(group.rank_of(ActorId::from_parts(0, 3)), Some(3));
        assert!(group.rank_of(ActorId::from_parts(0, 2)).is_none());
    }

    #[test]
    fn a_dying_actor_lists_the_groups_it_breaks() {
        let (registry, _) = registry_with_group(4);
        let groups = registry.groups_with(ActorId::from_parts(0, 1));
        assert_eq!(groups, vec!["g1".to_string()]);
        assert!(registry.groups_with(ActorId::generate()).is_empty());
    }

    #[test]
    fn destroyed_groups_are_inert() {
        let (mut registry, _) = registry_with_group(2);
        registry.destroy("g1");
        assert_eq!(registry.get("g1").unwrap().state, GroupState::Destroyed);
        assert!(registry.begin_rebuild("g1").is_none());
        assert!(registry.break_group("g1", "too late").is_empty());
        assert!(registry.groups_with(ActorId::from_parts(0, 0)).is_empty());
    }
}

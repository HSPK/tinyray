//! Python bindings for cluster state: resources, placement and the actor table.
//!
//! The head runs in the driver process for a single-machine cluster and as its
//! own binary for a multi-node one. Both use exactly this state machine, so the
//! local mode people develop against is the one that gets exercised.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tinyray_core::{ActorId, NodeId};
use tinyray_runtime::cluster::{
    ActorInfo, ActorState, ClusterState, NodeInfo, Resources, Strategy,
};

use crate::errors::TinyrayError;

fn parse_strategy(value: &str) -> PyResult<Strategy> {
    match value.to_ascii_uppercase().as_str() {
        "PACK" => Ok(Strategy::Pack),
        "SPREAD" => Ok(Strategy::Spread),
        "STRICT_SPREAD" => Ok(Strategy::StrictSpread),
        other => Err(PyValueError::new_err(format!(
            "unknown placement strategy {other:?}; expected PACK, SPREAD or STRICT_SPREAD"
        ))),
    }
}

fn parse_resources(
    num_cpus: f64,
    num_gpus: f64,
    memory_bytes: u64,
    custom: Option<HashMap<String, f64>>,
) -> Resources {
    Resources {
        num_cpus,
        num_gpus,
        memory_bytes,
        custom: custom.unwrap_or_default(),
    }
}

fn state_name(state: ActorState) -> &'static str {
    match state {
        ActorState::Starting => "STARTING",
        ActorState::Alive => "ALIVE",
        ActorState::Restarting => "RESTARTING",
        ActorState::Dead => "DEAD",
    }
}

/// The head's bookkeeping, as seen from Python.
#[pyclass(module = "tinyray._tinyray", name = "ClusterState")]
pub struct PyClusterState {
    inner: Arc<Mutex<ClusterState>>,
}

#[pymethods]
impl PyClusterState {
    #[new]
    #[pyo3(signature = (heartbeat_timeout_seconds=30.0))]
    fn py_new(heartbeat_timeout_seconds: f64) -> PyClusterState {
        PyClusterState {
            inner: Arc::new(Mutex::new(ClusterState::new(Duration::from_secs_f64(
                heartbeat_timeout_seconds,
            )))),
        }
    }

    #[pyo3(signature = (node_id, endpoint, hostname, num_cpus, num_gpus=0.0, memory_bytes=0, gpu_ids=None, custom=None))]
    #[allow(clippy::too_many_arguments)]
    fn register_node(
        &self,
        node_id: &str,
        endpoint: &str,
        hostname: &str,
        num_cpus: f64,
        num_gpus: f64,
        memory_bytes: u64,
        gpu_ids: Option<Vec<u32>>,
        custom: Option<HashMap<String, f64>>,
    ) -> PyResult<()> {
        let node_id: NodeId = node_id
            .parse()
            .map_err(|_| PyValueError::new_err(format!("invalid node id: {node_id}")))?;
        let resources = parse_resources(num_cpus, num_gpus, memory_bytes, custom);
        let free_gpus = gpu_ids.unwrap_or_else(|| (0..num_gpus as u32).collect());
        self.inner.lock().register_node(NodeInfo {
            node_id,
            endpoint: endpoint.to_string(),
            hostname: hostname.to_string(),
            total: resources.clone(),
            available: resources,
            free_gpus,
        });
        Ok(())
    }

    /// Reserve resources for one actor. Returns `(node_id, endpoint, gpu_ids)`.
    #[pyo3(signature = (num_cpus=1.0, num_gpus=0.0, memory_bytes=0, strategy="SPREAD", custom=None))]
    fn place(
        &self,
        num_cpus: f64,
        num_gpus: f64,
        memory_bytes: u64,
        strategy: &str,
        custom: Option<HashMap<String, f64>>,
    ) -> PyResult<(String, String, Vec<u32>)> {
        let resources = parse_resources(num_cpus, num_gpus, memory_bytes, custom);
        let strategy = parse_strategy(strategy)?;
        let mut state = self.inner.lock();
        let (node_id, gpu_ids) = state
            .place(&resources, strategy, &[])
            .map_err(|err| TinyrayError::new_err(err.to_string()))?;
        let endpoint = state
            .node(node_id)
            .map(|n| n.endpoint.clone())
            .unwrap_or_default();
        Ok((node_id.to_string(), endpoint, gpu_ids))
    }

    /// Reserve resources for `count` actors, all or nothing.
    #[pyo3(signature = (count, num_cpus=1.0, num_gpus=0.0, memory_bytes=0, strategy="SPREAD", custom=None))]
    fn place_gang(
        &self,
        count: usize,
        num_cpus: f64,
        num_gpus: f64,
        memory_bytes: u64,
        strategy: &str,
        custom: Option<HashMap<String, f64>>,
    ) -> PyResult<Vec<(String, String, Vec<u32>)>> {
        let resources = parse_resources(num_cpus, num_gpus, memory_bytes, custom);
        let strategy = parse_strategy(strategy)?;
        let mut state = self.inner.lock();
        let placements = state
            .place_gang(&resources, count, strategy)
            .map_err(|err| TinyrayError::new_err(err.to_string()))?;
        Ok(placements
            .into_iter()
            .map(|(node_id, gpu_ids)| {
                let endpoint = state
                    .node(node_id)
                    .map(|n| n.endpoint.clone())
                    .unwrap_or_default();
                (node_id.to_string(), endpoint, gpu_ids)
            })
            .collect())
    }

    #[pyo3(signature = (num_cpus=1.0, num_gpus=0.0, memory_bytes=0, strategy="SPREAD", custom=None))]
    fn gang_capacity(
        &self,
        num_cpus: f64,
        num_gpus: f64,
        memory_bytes: u64,
        strategy: &str,
        custom: Option<HashMap<String, f64>>,
    ) -> PyResult<usize> {
        let resources = parse_resources(num_cpus, num_gpus, memory_bytes, custom);
        Ok(self
            .inner
            .lock()
            .gang_capacity(&resources, parse_strategy(strategy)?))
    }

    #[pyo3(signature = (actor_id, node_id, endpoint, num_cpus=1.0, num_gpus=0.0, memory_bytes=0, gpu_ids=None, name=None, max_restarts=0, detached=false))]
    #[allow(clippy::too_many_arguments)]
    fn add_actor(
        &self,
        actor_id: &str,
        node_id: &str,
        endpoint: &str,
        num_cpus: f64,
        num_gpus: f64,
        memory_bytes: u64,
        gpu_ids: Option<Vec<u32>>,
        name: Option<String>,
        max_restarts: u32,
        detached: bool,
    ) -> PyResult<()> {
        let actor_id: ActorId = actor_id
            .parse()
            .map_err(|_| PyValueError::new_err(format!("invalid actor id: {actor_id}")))?;
        let node_id: NodeId = node_id
            .parse()
            .map_err(|_| PyValueError::new_err(format!("invalid node id: {node_id}")))?;
        self.inner.lock().add_actor(ActorInfo {
            actor_id,
            name,
            node_id,
            endpoint: endpoint.to_string(),
            state: ActorState::Alive,
            resources: parse_resources(num_cpus, num_gpus, memory_bytes, None),
            gpu_ids: gpu_ids.unwrap_or_default(),
            restarts: 0,
            max_restarts,
            detached,
        });
        Ok(())
    }

    /// Record a death; returns True if the actor should be restarted.
    fn note_actor_died(&self, actor_id: &str) -> PyResult<bool> {
        let actor_id: ActorId = actor_id
            .parse()
            .map_err(|_| PyValueError::new_err(format!("invalid actor id: {actor_id}")))?;
        Ok(self.inner.lock().note_actor_died(actor_id))
    }

    fn set_actor_endpoint(&self, actor_id: &str, endpoint: &str) -> PyResult<()> {
        let actor_id: ActorId = actor_id
            .parse()
            .map_err(|_| PyValueError::new_err(format!("invalid actor id: {actor_id}")))?;
        let mut state = self.inner.lock();
        state.set_actor_endpoint(actor_id, endpoint.to_string());
        state.set_actor_state(actor_id, ActorState::Alive);
        Ok(())
    }

    fn remove_actor(&self, actor_id: &str) -> PyResult<bool> {
        let actor_id: ActorId = actor_id
            .parse()
            .map_err(|_| PyValueError::new_err(format!("invalid actor id: {actor_id}")))?;
        Ok(self.inner.lock().remove_actor(actor_id).is_some())
    }

    fn actor_by_name(&self, name: &str) -> Option<String> {
        self.inner
            .lock()
            .actor_by_name(name)
            .map(|actor| actor.actor_id.to_string())
    }

    fn actor<'py>(&self, py: Python<'py>, actor_id: &str) -> PyResult<Option<Bound<'py, PyDict>>> {
        let actor_id: ActorId = actor_id
            .parse()
            .map_err(|_| PyValueError::new_err(format!("invalid actor id: {actor_id}")))?;
        let state = self.inner.lock();
        let Some(actor) = state.actor(actor_id) else {
            return Ok(None);
        };
        Ok(Some(actor_to_dict(py, actor)?))
    }

    fn actors<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        self.inner
            .lock()
            .actors()
            .iter()
            .map(|actor| actor_to_dict(py, actor))
            .collect()
    }

    fn nodes<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        self.inner
            .lock()
            .nodes()
            .iter()
            .map(|node| {
                let dict = PyDict::new(py);
                dict.set_item("node_id", node.node_id.to_string())?;
                dict.set_item("endpoint", &node.endpoint)?;
                dict.set_item("hostname", &node.hostname)?;
                dict.set_item("total_cpus", node.total.num_cpus)?;
                dict.set_item("total_gpus", node.total.num_gpus)?;
                dict.set_item("available_cpus", node.available.num_cpus)?;
                dict.set_item("available_gpus", node.available.num_gpus)?;
                dict.set_item("free_gpu_ids", node.free_gpus.clone())?;
                Ok(dict)
            })
            .collect()
    }

    fn heartbeat(
        &self,
        node_id: &str,
        num_cpus: f64,
        num_gpus: f64,
        free_gpu_ids: Vec<u32>,
    ) -> PyResult<bool> {
        let node_id: NodeId = node_id
            .parse()
            .map_err(|_| PyValueError::new_err(format!("invalid node id: {node_id}")))?;
        Ok(self.inner.lock().heartbeat(
            node_id,
            parse_resources(num_cpus, num_gpus, 0, None),
            free_gpu_ids,
        ))
    }

    /// Nodes that missed their heartbeat deadline.
    fn dead_nodes(&self) -> Vec<String> {
        self.inner
            .lock()
            .dead_nodes()
            .into_iter()
            .map(|id| id.to_string())
            .collect()
    }

    /// Drop a node; returns the actors that died with it.
    fn remove_node(&self, node_id: &str) -> PyResult<Vec<String>> {
        let node_id: NodeId = node_id
            .parse()
            .map_err(|_| PyValueError::new_err(format!("invalid node id: {node_id}")))?;
        Ok(self
            .inner
            .lock()
            .remove_node(node_id)
            .into_iter()
            .map(|id| id.to_string())
            .collect())
    }

    fn release(
        &self,
        node_id: &str,
        num_cpus: f64,
        num_gpus: f64,
        gpu_ids: Vec<u32>,
    ) -> PyResult<()> {
        let node_id: NodeId = node_id
            .parse()
            .map_err(|_| PyValueError::new_err(format!("invalid node id: {node_id}")))?;
        self.inner.lock().release(
            node_id,
            &parse_resources(num_cpus, num_gpus, 0, None),
            &gpu_ids,
        );
        Ok(())
    }
}

fn actor_to_dict<'py>(py: Python<'py>, actor: &ActorInfo) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("actor_id", actor.actor_id.to_string())?;
    dict.set_item("name", actor.name.clone())?;
    dict.set_item("node_id", actor.node_id.to_string())?;
    dict.set_item("endpoint", &actor.endpoint)?;
    dict.set_item("state", state_name(actor.state))?;
    dict.set_item("num_cpus", actor.resources.num_cpus)?;
    dict.set_item("num_gpus", actor.resources.num_gpus)?;
    dict.set_item("gpu_ids", actor.gpu_ids.clone())?;
    dict.set_item("restarts", actor.restarts)?;
    dict.set_item("max_restarts", actor.max_restarts)?;
    dict.set_item("detached", actor.detached)?;
    Ok(dict)
}

/// Detect the GPUs on this machine.
///
/// Shelling out to `nvidia-smi` rather than linking CUDA keeps the Rust side
/// free of the most painful build dependency there is; NCCL lives entirely on
/// the Python/torch side.
#[pyfunction]
pub fn detect_gpus() -> PyResult<Vec<u32>> {
    if let Ok(visible) = std::env::var("CUDA_VISIBLE_DEVICES") {
        if visible.trim().is_empty() {
            return Ok(vec![]);
        }
        let parsed: Result<Vec<u32>, _> = visible
            .split(',')
            .map(|s| s.trim().parse::<u32>())
            .collect();
        if let Ok(ids) = parsed {
            return Ok((0..ids.len() as u32).collect());
        }
    }

    let output = std::process::Command::new("nvidia-smi")
        .arg("--query-gpu=index")
        .arg("--format=csv,noheader")
        .output();
    match output {
        Ok(output) if output.status.success() => Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| line.trim().parse::<u32>().ok())
            .collect()),
        // No nvidia-smi means no GPUs, which is a perfectly normal CPU-only
        // development machine rather than an error.
        _ => Ok(vec![]),
    }
}

/// Physical CPU count, used when a node agent reports its capacity.
#[pyfunction]
pub fn detect_cpus() -> PyResult<usize> {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .map_err(|err| PyRuntimeError::new_err(format!("failed to read CPU count: {err}")))
}

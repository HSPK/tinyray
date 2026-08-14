//! Python bindings for collective group bookkeeping.
//!
//! Note what is *not* here: any broadcast implementation. tinyray assigns
//! ranks, distributes rendezvous information and manages the epoch state
//! machine; `torch.distributed` moves the bytes.

use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tinyray_core::ActorId;
use tinyray_runtime::collective::{CollectiveRegistry, GroupState};

use crate::errors::TinyrayError;

fn parse_actor(value: &str) -> PyResult<ActorId> {
    value
        .parse()
        .map_err(|_| PyValueError::new_err(format!("invalid actor id: {value}")))
}

fn state_name(state: GroupState) -> &'static str {
    match state {
        GroupState::Forming => "FORMING",
        GroupState::Ready => "READY",
        GroupState::Broken => "BROKEN",
        GroupState::Destroyed => "DESTROYED",
    }
}

/// The head's registry of collective groups.
#[pyclass(module = "tinyray._tinyray", name = "CollectiveRegistry")]
pub struct PyCollectiveRegistry {
    inner: Arc<Mutex<CollectiveRegistry>>,
}

#[pymethods]
impl PyCollectiveRegistry {
    #[new]
    fn py_new() -> PyCollectiveRegistry {
        PyCollectiveRegistry {
            inner: Arc::new(Mutex::new(CollectiveRegistry::new())),
        }
    }

    /// Validate a membership and assign ranks.
    ///
    /// `members` is a list of `(actor_id, num_gpus, node_id, gpu_ids, alive)`.
    #[pyo3(signature = (group_id, members, backend="nccl", store_host="127.0.0.1", store_port=29500))]
    fn create(
        &self,
        group_id: &str,
        members: Vec<(String, f64, String, Vec<u32>, bool)>,
        backend: &str,
        store_host: &str,
        store_port: u16,
    ) -> PyResult<usize> {
        let mut candidates = Vec::with_capacity(members.len());
        for (actor_id, num_gpus, node_id, gpu_ids, alive) in members {
            candidates.push((parse_actor(&actor_id)?, num_gpus, node_id, gpu_ids, alive));
        }
        let group = self
            .inner
            .lock()
            .create(
                group_id.to_string(),
                candidates,
                backend.to_string(),
                store_host.to_string(),
                store_port,
            )
            .map_err(|err| TinyrayError::new_err(err.to_string()))?;
        Ok(group.world_size())
    }

    /// Everything a member needs in order to call `init_process_group`.
    fn rendezvous_for<'py>(
        &self,
        py: Python<'py>,
        group_id: &str,
        actor_id: &str,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let actor_id = parse_actor(actor_id)?;
        let registry = self.inner.lock();
        let Some(group) = registry.get(group_id) else {
            return Ok(None);
        };
        let Some(rendezvous) = group.rendezvous_for(actor_id) else {
            return Ok(None);
        };
        let dict = PyDict::new(py);
        dict.set_item("group_id", rendezvous.group_id)?;
        dict.set_item("epoch", rendezvous.epoch)?;
        dict.set_item("rank", rendezvous.rank)?;
        dict.set_item("world_size", rendezvous.world_size)?;
        dict.set_item("store_host", rendezvous.store_host)?;
        dict.set_item("store_port", rendezvous.store_port)?;
        dict.set_item("backend", rendezvous.backend)?;
        Ok(Some(dict))
    }

    /// Record that a member finished joining the current epoch.
    fn acknowledge(&self, group_id: &str, actor_id: &str, epoch: u64) -> PyResult<Option<String>> {
        let actor_id = parse_actor(actor_id)?;
        Ok(self
            .inner
            .lock()
            .acknowledge(group_id, actor_id, epoch)
            .map(|state| state_name(state).to_string()))
    }

    /// Mark a group unusable; returns the members that must abort.
    fn break_group(&self, group_id: &str, reason: &str) -> Vec<String> {
        self.inner
            .lock()
            .break_group(group_id, reason)
            .into_iter()
            .map(|id| id.to_string())
            .collect()
    }

    /// Bump the epoch and start forming again.
    fn begin_rebuild(&self, group_id: &str) -> Option<u64> {
        self.inner.lock().begin_rebuild(group_id).map(|g| g.epoch)
    }

    fn replace_member(
        &self,
        group_id: &str,
        old: &str,
        new: &str,
        node_id: &str,
        gpu_ids: Vec<u32>,
    ) -> PyResult<bool> {
        Ok(self.inner.lock().replace_member(
            group_id,
            parse_actor(old)?,
            parse_actor(new)?,
            node_id.to_string(),
            gpu_ids,
        ))
    }

    /// Groups an actor belongs to; used when it dies.
    fn groups_with(&self, actor_id: &str) -> PyResult<Vec<String>> {
        Ok(self.inner.lock().groups_with(parse_actor(actor_id)?))
    }

    fn destroy(&self, group_id: &str) -> Vec<String> {
        self.inner
            .lock()
            .destroy(group_id)
            .into_iter()
            .map(|id| id.to_string())
            .collect()
    }

    fn info<'py>(&self, py: Python<'py>, group_id: &str) -> PyResult<Option<Bound<'py, PyDict>>> {
        let registry = self.inner.lock();
        let Some(group) = registry.get(group_id) else {
            return Ok(None);
        };
        let dict = PyDict::new(py);
        dict.set_item("group_id", &group.group_id)?;
        dict.set_item("epoch", group.epoch)?;
        dict.set_item("state", state_name(group.state))?;
        dict.set_item("backend", &group.backend)?;
        dict.set_item("world_size", group.world_size())?;
        dict.set_item("acknowledged", group.acknowledged())?;
        dict.set_item("store_host", &group.store_host)?;
        dict.set_item("store_port", group.store_port)?;
        dict.set_item(
            "members",
            group
                .members
                .iter()
                .map(|m| {
                    (
                        m.actor_id.to_string(),
                        m.rank,
                        m.node_id.clone(),
                        m.gpu_ids.clone(),
                    )
                })
                .collect::<Vec<_>>(),
        )?;
        Ok(Some(dict))
    }

    fn group_ids(&self) -> Vec<String> {
        self.inner.lock().group_ids()
    }
}

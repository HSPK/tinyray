//! PyO3 bindings exposing the tinyray Rust core to Python.
//!
//! The module is intentionally thin: it owns no policy, only the translation
//! between Python objects and the core types. See [`buffers`] for the copy
//! rules at the language boundary.

use pyo3::prelude::*;

pub mod bench;
pub mod buffers;
pub mod cluster;
pub mod collective;
pub mod driver;
pub mod errors;
pub mod framing;
pub mod ids;
pub mod runtime;
pub mod worker;

#[pymodule]
fn _tinyray(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__doc__", "Rust core for tinyray.")?;
    module.add("version", env!("CARGO_PKG_VERSION"))?;

    module.add(
        "TinyrayError",
        module.py().get_type::<errors::TinyrayError>(),
    )?;
    module.add(
        "ProtocolError",
        module.py().get_type::<errors::ProtocolError>(),
    )?;
    module.add(
        "MessageTooLarge",
        module.py().get_type::<errors::MessageTooLarge>(),
    )?;

    module.add_class::<buffers::Frame>()?;
    module.add_class::<framing::PyLimits>()?;
    module.add_class::<framing::PyDecoder>()?;
    module.add_function(wrap_pyfunction!(framing::encode_message, module)?)?;
    module.add_function(wrap_pyfunction!(framing::decode_message, module)?)?;

    module.add_function(wrap_pyfunction!(bench::bench_decode_native, module)?)?;

    for (name, exc) in [
        (
            "RemoteCallError",
            module.py().get_type::<errors::RemoteCallError>(),
        ),
        (
            "UserCodeError",
            module.py().get_type::<errors::UserCodeError>(),
        ),
        ("ObjectLost", module.py().get_type::<errors::ObjectLost>()),
        ("ActorDied", module.py().get_type::<errors::ActorDied>()),
        ("NotFound", module.py().get_type::<errors::NotFound>()),
        (
            "Backpressure",
            module.py().get_type::<errors::Backpressure>(),
        ),
    ] {
        module.add(name, exc)?;
    }

    module.add_class::<worker::PyActorRuntime>()?;
    module.add_class::<worker::PyTask>()?;
    module.add_class::<driver::PyClientRuntime>()?;
    module.add_class::<driver::PyOwnerRef>()?;

    module.add_class::<cluster::PyClusterState>()?;
    module.add_class::<collective::PyCollectiveRegistry>()?;
    module.add_function(wrap_pyfunction!(cluster::detect_gpus, module)?)?;
    module.add_function(wrap_pyfunction!(cluster::detect_cpus, module)?)?;

    module.add_class::<ids::PyId>()?;
    module.add_function(wrap_pyfunction!(ids::new_id, module)?)?;

    Ok(())
}

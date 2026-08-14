//! Benchmark helpers exposed to Python.
//!
//! These exist to model the *server* side of the data path honestly. In the
//! real runtime a result fetch is handled end to end by a tokio worker thread
//! that never touches the interpreter, so measuring a decode that Python
//! initiates would conflate two very different things:
//!
//! * the cost of the decode itself (Rust, GIL-free), and
//! * the latency of acquiring the GIL to enter and leave the binding.
//!
//! The second is unavoidable for anything Python calls, and it is precisely why
//! the design forbids driving the serving path from Python.

use std::time::Instant;

use bytes::BytesMut;
use pyo3::prelude::*;
use tinyray_core::framing::Decoder as CoreDecoder;
use tinyray_core::Limits;

use crate::buffers::bytes_from_py;

/// Decode `repeats` copies of `data` on a dedicated native thread and return
/// the median duration in seconds.
///
/// The calling thread drops the GIL for the whole run, and the worker thread
/// never acquires it, so the result is unaffected by other Python threads.
/// This is what the tokio server path looks like.
#[pyfunction]
#[pyo3(signature = (data, repeats=50))]
pub fn bench_decode_native(
    py: Python<'_>,
    data: &Bound<'_, PyAny>,
    repeats: usize,
) -> PyResult<f64> {
    let raw = bytes_from_py(data)?;
    let repeats = repeats.max(1);

    let median = py.detach(move || {
        std::thread::spawn(move || {
            let mut samples = Vec::with_capacity(repeats);
            for _ in 0..repeats {
                let started = Instant::now();
                let mut buf = BytesMut::from(&raw[..]);
                let mut decoder = CoreDecoder::new(Limits::DEFAULT);
                let message = decoder.decode(&mut buf).expect("valid benchmark input");
                // Keep the optimiser honest.
                std::hint::black_box(&message);
                samples.push(started.elapsed().as_secs_f64());
            }
            samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
            samples[samples.len() / 2]
        })
        .join()
    });

    median
        .map_err(|_| pyo3::exceptions::PyRuntimeError::new_err("benchmark worker thread panicked"))
}

//! Mapping Rust errors onto Python exceptions.
//!
//! Cross-language error legibility is called out as a risk in the design doc:
//! a Rust error that surfaces as an opaque string is a debugging dead end. Every
//! error the core can produce gets a named Python exception here.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::PyAnyMethods;
use pyo3::{PyErr, Python};
use tinyray_core::proto::{ErrorKind, RemoteError};
use tinyray_core::FrameError;

create_exception!(
    _tinyray,
    TinyrayError,
    PyException,
    "Base class for every tinyray error."
);
create_exception!(
    _tinyray,
    ProtocolError,
    TinyrayError,
    "The peer sent something that does not conform to the tinyray wire protocol."
);
create_exception!(
    _tinyray,
    MessageTooLarge,
    ProtocolError,
    "A message exceeded the configured size limits."
);

/// Convert a framing error into the matching Python exception.
pub fn frame_error_to_py(err: FrameError) -> PyErr {
    let message = err.to_string();
    match err {
        FrameError::HeaderTooLarge { .. }
        | FrameError::TooManyFrames { .. }
        | FrameError::FrameTooLarge { .. }
        | FrameError::MessageTooLarge { .. } => MessageTooLarge::new_err(message),
        FrameError::BadMagic { .. } | FrameError::Poisoned => ProtocolError::new_err(message),
    }
}

create_exception!(
    _tinyray,
    RemoteCallError,
    TinyrayError,
    "A call failed on the remote actor."
);
create_exception!(
    _tinyray,
    UserCodeError,
    RemoteCallError,
    "The user's method raised. The remote traceback is attached."
);
create_exception!(
    _tinyray,
    ObjectLost,
    RemoteCallError,
    "The result was evicted, expired, or its owner restarted."
);
create_exception!(
    _tinyray,
    ActorDied,
    RemoteCallError,
    "The target actor is no longer alive."
);
create_exception!(
    _tinyray,
    NotFound,
    RemoteCallError,
    "No such actor, task or method."
);
create_exception!(
    _tinyray,
    Backpressure,
    RemoteCallError,
    "The target is over its queue or memory watermark."
);

/// Turn a wire error into the matching Python exception.
///
/// The remote traceback is appended to the message rather than hidden in an
/// attribute: in a distributed run, the stack that matters is the remote one,
/// and burying it behind an attribute means nobody reads it.
pub fn remote_error_to_py(err: &RemoteError) -> PyErr {
    let mut message = format!("[{}] {}", err.kind.as_str(), err.message);
    if let Some(traceback) = &err.traceback {
        message.push_str("\n--- remote traceback ---\n");
        message.push_str(traceback);
    }
    let exception = match err.kind {
        ErrorKind::UserException => UserCodeError::new_err(message),
        ErrorKind::ObjectLost => ObjectLost::new_err(message),
        ErrorKind::ActorDied => ActorDied::new_err(message),
        ErrorKind::NotFound => NotFound::new_err(message),
        ErrorKind::Backpressure => Backpressure::new_err(message),
        ErrorKind::Internal => RemoteCallError::new_err(message),
    };
    Python::attach(|py| {
        if let Some(traceback) = &err.traceback {
            let _ = exception
                .value(py)
                .setattr("remote_traceback", traceback.as_str());
        }
        let _ = exception.value(py).setattr("kind", err.kind.as_str());
    });
    exception
}

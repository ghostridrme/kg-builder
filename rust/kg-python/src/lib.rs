//! Blocking Python calls await the same Rust engine used by other hosts.
mod bootstrap;
mod config;
mod outcome;
mod rules;
mod runtime;
mod telemetry;

use pyo3::prelude::*;
pyo3::create_exception!(_native, ConfigurationError, pyo3::exceptions::PyValueError);
pyo3::create_exception!(
    _native,
    InputValidationError,
    pyo3::exceptions::PyValueError
);
pyo3::create_exception!(_native, EngineClosedError, pyo3::exceptions::PyRuntimeError);
pyo3::create_exception!(_native, ForkedEngineError, pyo3::exceptions::PyRuntimeError);

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    // Version 3 requires the bounded-request constructor and checkpoint operations.
    m.add("PROTOCOL_VERSION", 4)?;
    m.add("BUILD_FLAVOR", "deterministic")?;
    m.add(
        "ConfigurationError",
        m.py().get_type::<ConfigurationError>(),
    )?;
    m.add(
        "InputValidationError",
        m.py().get_type::<InputValidationError>(),
    )?;
    m.add("EngineClosedError", m.py().get_type::<EngineClosedError>())?;
    m.add("ForkedEngineError", m.py().get_type::<ForkedEngineError>())?;
    // Public exception types must resolve through the installed extension package,
    // including when Python serializes an exception for another process.
    for name in [
        "ConfigurationError",
        "InputValidationError",
        "EngineClosedError",
        "ForkedEngineError",
    ] {
        m.getattr(name)?.setattr("__module__", "kg_sdk._native")?;
    }
    m.add_class::<runtime::NativeEngine>()?;
    m.add_class::<runtime::CancellationToken>()?;
    m.add_class::<runtime::TelemetryHandle>()?;
    Ok(())
}

mod profiles;

mod checkpoints;

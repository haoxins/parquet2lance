use parquet2lance::io::{p2l as _p2l, Reader};
use pyo3::prelude::*;
use tokio;

use std::path::PathBuf;

#[pyfunction]
fn p2l(input: String, output: String) -> PyResult<()> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let reader = Reader::new(&PathBuf::from(input), true).await;
        _p2l(reader, &PathBuf::from(output), true).await;
    });
    Ok(())
}

#[pymodule]
#[pyo3(name = "parquet2lance")]
fn python(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(p2l, m)?)?;
    Ok(())
}

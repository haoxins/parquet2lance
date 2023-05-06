use arrow_array::RecordBatchReader;
use lance::dataset::Dataset;
use lance::dataset::{WriteMode, WriteParams};

use std::path::Path;

pub use crate::io::reader::Reader;

mod fs;
mod gcs;
mod reader;
mod util;

trait StorageReader {
    async fn next(&mut self) -> Option<Box<dyn RecordBatchReader>>;
}

pub async fn p2l(mut reader: Reader, output_dir: &Path, overwrite: bool) {
    let mut initialized = false;

    while let Some(mut f) = reader.next().await {
        let output_dir = output_dir.to_str().unwrap();

        let write_params = get_write_params(initialized, overwrite);

        Dataset::write(&mut f, output_dir, Some(write_params))
            .await
            .unwrap();

        initialized = true;
    }
}

fn get_write_params(initialized: bool, overwrite: bool) -> WriteParams {
    let mut params = WriteParams::default();

    if !initialized {
        params.mode = if overwrite {
            WriteMode::Overwrite
        } else {
            WriteMode::Create
        };
        return params;
    }

    params.mode = WriteMode::Append;

    params
}

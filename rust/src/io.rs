use lance::dataset::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::errors::Result as ParquetResult;

use std::path::Path;

pub use crate::io::reader::Reader;

mod fs;
mod gcs;
mod reader;
mod util;

trait StorageReader {
    async fn next(&mut self) -> ParquetResult<ParquetRecordBatchReader>;
}

pub async fn p2l(mut reader: Reader, output_dir: &Path, overwrite: bool) {
    let mut initialized = false;

    loop {
        let result = reader.next().await;
        match result {
            Ok(reader) => {
                let output_dir = output_dir.to_str().unwrap();

                let write_params = get_write_params(initialized, overwrite);
                let result = Dataset::write(reader, output_dir, Some(write_params)).await;

                match result {
                    Ok(_) => (),
                    Err(e) => {
                        panic!("Error writing record: {:?}", e);
                    }
                }
            }
            Err(e) => {
                if e.to_string().contains("No more files") {
                    break;
                }
                panic!("Error reading record: {:?}", e);
            }
        }

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

use arrow_array::RecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use std::fs::read_dir;
use std::fs::File;
use std::path::PathBuf;

pub fn read_file(file_path: &PathBuf) -> Box<dyn RecordBatchReader> {
    let file = File::open(file_path).unwrap();

    Box::new(
        ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(8192)
            .build()
            .unwrap(),
    )
}

pub fn get_input_files(input: &PathBuf) -> Vec<PathBuf> {
    let mut files = vec![];

    if input.is_file() {
        files.push(input.clone());
        return files;
    }

    if input.is_dir() {
        let mut dir = read_dir(input).unwrap();
        while let Some(Ok(entry)) = dir.next() {
            // We don't handle directories yet
            if entry.path().is_file() {
                files.push(entry.path());
            }
        }

        return files;
    }

    panic!("Input path is neither a file nor a directory");
}

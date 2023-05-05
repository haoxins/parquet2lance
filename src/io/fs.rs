use arrow_array::RecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use std::fs::read_dir;
use std::fs::File;
use std::path::PathBuf;

use crate::io::StorageReader;

pub struct FsReader {
    verbose: bool,
    file_list: Vec<PathBuf>,
}

impl FsReader {
    pub fn new(path: &PathBuf, verbose: bool) -> Self {
        Self {
            verbose,
            file_list: get_file_list(path),
        }
    }
}

impl StorageReader for FsReader {
    async fn next(&mut self) -> Option<Box<dyn RecordBatchReader>> {
        if self.file_list.is_empty() {
            return None;
        }

        let file_path = self.file_list.remove(0);
        if self.verbose {
            println!("Reading file {:?}", &file_path);
        }
        let file = File::open(file_path).unwrap();

        let r = Box::new(
            ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .with_batch_size(8192)
                .build()
                .unwrap(),
        );

        Some(r)
    }
}

fn get_file_list(input: &PathBuf) -> Vec<PathBuf> {
    let mut file_list = vec![];

    if input.is_file() {
        file_list.push(input.clone());
    }

    if input.is_dir() {
        let mut dir = read_dir(input).unwrap();
        while let Some(Ok(entry)) = dir.next() {
            // We don't handle directories yet
            if entry.path().is_file() {
                file_list.push(entry.path());
            }
        }
    }

    file_list
}

use arrow_array::RecordBatchReader;

use std::path::PathBuf;

use super::fs;
use super::gcs;

#[derive(PartialEq)]
enum FileScheme {
    FS,
    GCS,
}

pub trait StorageReader {
    async fn next(&mut self) -> Option<Box<dyn RecordBatchReader>>;
}

pub struct Reader {
    scheme: FileScheme,
    storage_reader: dyn StorageReader,
}

impl Reader {
    pub async fn new(path: &PathBuf, verbose: bool) -> Self {
        if path.starts_with("gs://") {
            return Self {
                scheme: FileScheme::GCS,
                storage_reader: gcs::GcsReader::new(path, verbose).await,
            };
        }

        Self {
            scheme: FileScheme::FS,
            storage_reader: fs::FsReader::new(path, verbose),
        }
    }

    pub async fn next(&mut self) -> Option<Box<dyn RecordBatchReader>> {
        self.storage_reader().next().await
    }
}

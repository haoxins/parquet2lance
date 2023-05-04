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
    fs_reader: Option<fs::FsReader>,
    gcs_reader: Option<gcs::GcsReader>,
}

impl Reader {
    pub async fn new(path: &PathBuf, verbose: bool) -> Self {
        if path.starts_with("gs://") {
            return Self {
                scheme: FileScheme::GCS,
                fs_reader: None,
                gcs_reader: Some(gcs::GcsReader::new(path, verbose).await),
            };
        }

        Self {
            scheme: FileScheme::FS,
            fs_reader: Some(fs::FsReader::new(path, verbose)),
            gcs_reader: None,
        }
    }

    pub async fn next(&mut self) -> Option<Box<dyn RecordBatchReader>> {
        match self.scheme {
            FileScheme::FS => self.fs_reader.as_mut().unwrap().next().await,
            FileScheme::GCS => self.gcs_reader.as_mut().unwrap().next().await,
        }
    }
}

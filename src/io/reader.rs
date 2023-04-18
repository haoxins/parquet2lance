use arrow_array::RecordBatchReader;

use std::path::PathBuf;

use super::fs;
use super::gcs;

#[derive(PartialEq)]
enum FileScheme {
    FS,
    GCS,
}

pub struct Reader {
    scheme: FileScheme,
    file_list: Vec<PathBuf>,
}

impl Reader {
    pub async fn new(path: &PathBuf) -> Self {
        if path.starts_with("gs://") {
            return Self {
                scheme: FileScheme::GCS,
                file_list: gcs::get_file_list(path).await,
            };
        }

        Self {
            scheme: FileScheme::FS,
            file_list: fs::get_file_list(path),
        }
    }

    pub async fn next(&mut self) -> Option<Box<dyn RecordBatchReader>> {
        if self.file_list.is_empty() {
            return None;
        }

        let p = self.file_list.remove(0);

        if self.scheme == FileScheme::GCS {
            return Some(gcs::read_file(&p).await);
        }

        Some(fs::read_file(&p))
    }
}

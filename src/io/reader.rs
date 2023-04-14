use arrow_array::RecordBatchReader;

use std::path::PathBuf;

use super::fs;

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
    pub fn new(path: &PathBuf) -> Self {
        if path.starts_with("gs://") {
            return Self {
                scheme: FileScheme::GCS,
                file_list: vec![],
            };
        }

        Self {
            scheme: FileScheme::FS,
            file_list: fs::get_file_list(path),
        }
    }

    pub fn next(&mut self) -> Option<Box<dyn RecordBatchReader>> {
        if self.file_list.is_empty() {
            return None;
        }

        let p = self.file_list.remove(0);

        if self.scheme == FileScheme::GCS {
            println!("TODO");
        }

        Some(fs::read_file(&p))
    }
}

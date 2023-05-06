use arrow_array::RecordBatchReader;

pub mod reader;

mod fs;
mod gcs;
mod util;

trait StorageReader {
    async fn next(&mut self) -> Option<Box<dyn RecordBatchReader>>;
}

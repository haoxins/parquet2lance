use arrow_array::RecordBatchReader;
use futures::stream::StreamExt;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{path::Path as ObjectStorePath, ObjectStore};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use std::path::PathBuf;
use std::sync::Arc;

use crate::io::reader::StorageReader;
use crate::io::util::get_bucket_name;

pub struct GcsReader {
    verbose: bool,
    file_list: Vec<PathBuf>,
}

impl GcsReader {
    pub async fn new(path: &PathBuf, verbose: bool) -> Self {
        Self {
            verbose,
            file_list: get_file_list(path, verbose).await,
        }
    }
}

impl StorageReader for GcsReader {
    async fn next(&mut self) -> Option<Box<dyn RecordBatchReader>> {
        if self.file_list.is_empty() {
            return None;
        }

        let file_path = self.file_list.remove(0);

        let bucket_name = get_bucket_name(&file_path).unwrap();
        let client = get_gcs_client(bucket_name);

        if self.verbose {
            println!("Reading GCS object {:?}", &file_path);
        }

        let p = file_path.clone().into_os_string().into_string().unwrap();
        let object = client
            .get(&ObjectStorePath::parse(p).unwrap())
            .await
            .unwrap();
        let data = object.bytes().await.unwrap();

        let r = ParquetRecordBatchReaderBuilder::try_new(data)
            .unwrap()
            .with_batch_size(8192)
            .build()
            .unwrap();

        Some(Box::new(r))
    }
}

pub async fn get_file_list(prefix: &PathBuf, verbose: bool) -> Vec<PathBuf> {
    let p: ObjectStorePath = prefix.to_str().unwrap().try_into().unwrap();
    let bucket_name = get_bucket_name(prefix).unwrap();
    let client = get_gcs_client(bucket_name);

    if verbose {
        println!("Getting GCS object list from {}", &p);
    }

    let results = client.list(Some(&p)).await.unwrap().map(|meta| {
        let p = meta.unwrap().location.to_string();
        if verbose {
            println!("Converting parquet file {}", &p);
        }
        PathBuf::from(p)
    });

    results.collect().await
}

fn get_gcs_client(bucket_name: String) -> Arc<dyn ObjectStore> {
    let builder = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(bucket_name)
        .build();

    let client: Arc<dyn ObjectStore> = match builder {
        Ok(client) => Arc::new(client),
        Err(e) => panic!("Failed to create GCS client, {}", e),
    };

    client
}

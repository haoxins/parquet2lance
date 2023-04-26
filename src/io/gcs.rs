use arrow_array::RecordBatchReader;
use futures::stream::StreamExt;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{path::Path as ObjectStorePath, ObjectStore};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use std::path::PathBuf;
use std::sync::Arc;

use crate::io::util::get_bucket_name;

pub async fn read_file(file_path: &PathBuf) -> Box<dyn RecordBatchReader> {
    let client = get_gcs_client(file_path);

    let p = file_path.clone().into_os_string().into_string().unwrap();
    let object = client
        .get(&ObjectStorePath::parse(p).unwrap())
        .await
        .unwrap();
    let data = object.bytes().await.unwrap();

    Box::new(
        ParquetRecordBatchReaderBuilder::try_new(data)
            .unwrap()
            .with_batch_size(8192)
            .build()
            .unwrap(),
    )
}

pub async fn get_file_list(prefix: &PathBuf) -> Vec<PathBuf> {
    let p: ObjectStorePath = prefix.to_str().unwrap().try_into().unwrap();

    let client = get_gcs_client(prefix);

    let results = client
        .list(Some(&p))
        .await
        .unwrap()
        .map(|meta| PathBuf::from(meta.unwrap().location.to_string().as_str()));

    results.collect().await
}

fn get_gcs_client(prefix: &PathBuf) -> Arc<dyn ObjectStore> {
    let bucket_name = get_bucket_name(prefix).unwrap();

    let builder = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(bucket_name)
        .build();

    let client: Arc<dyn ObjectStore> = match builder {
        Ok(client) => Arc::new(client),
        Err(e) => panic!("Failed to create GCS client, {}", e),
    };

    client
}

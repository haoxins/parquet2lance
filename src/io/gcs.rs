use arrow_array::RecordBatchReader;
use futures::stream::StreamExt;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{path::Path as ObjectStorePath, ObjectStore};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use std::path::PathBuf;
use std::sync::Arc;

pub async fn read_file(file_path: &PathBuf) -> Box<dyn RecordBatchReader> {
    let gcs: Arc<dyn ObjectStore> =
        Arc::new(GoogleCloudStorageBuilder::from_env().build().unwrap());

    let p = file_path.clone().into_os_string().into_string().unwrap();
    let object = gcs.get(&ObjectStorePath::parse(p).unwrap()).await.unwrap();
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

    let gcs: Arc<dyn ObjectStore> =
        Arc::new(GoogleCloudStorageBuilder::from_env().build().unwrap());

    let results = gcs
        .list(Some(&p))
        .await
        .unwrap()
        .map(|meta| PathBuf::from(meta.unwrap().location.to_string().as_str()));

    results.collect().await
}

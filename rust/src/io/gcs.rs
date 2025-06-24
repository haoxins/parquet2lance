use futures::stream::StreamExt;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{ObjectStore, path::Path as ObjectStorePath};
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::{ParquetError, Result as ParquetResult};

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::io::StorageReader;
use crate::io::util::{
    get_bucket_name, get_object_path, get_object_prefix, is_parquet_object_path,
};

pub struct GcsReader {
    verbose: bool,
    bucket_name: String,
    file_list: Vec<PathBuf>,
}

impl GcsReader {
    pub async fn new(path: &Path, verbose: bool) -> Self {
        Self {
            verbose,
            bucket_name: get_bucket_name(path).unwrap(),
            file_list: Self::get_file_list(path, verbose).await,
        }
    }

    async fn get_file_list(p: &Path, verbose: bool) -> Vec<PathBuf> {
        let bucket_name = get_bucket_name(p).unwrap();

        if is_parquet_object_path(p) {
            return vec![get_object_path(p).unwrap()];
        }

        let prefix = get_object_prefix(p);
        let client = Self::get_gcs_client(&bucket_name);

        if verbose {
            println!("Getting GCS objects from {:?}/{:?}", &bucket_name, &prefix.as_ref().unwrap());
        }

        let objects = client
            .list(prefix.as_ref())
            .filter_map(|meta_result| async {
                let meta = meta_result.ok()?;
                let p = meta.location.to_string();
                let is_valid = p.ends_with(".parquet") && meta.size > 0;

                if verbose {
                    println!("Found file {:?}, valid: {:?}", &p, is_valid);
                }

                if is_valid {
                    Some(PathBuf::from(p))
                } else {
                    None
                }
            })
            .collect::<Vec<PathBuf>>()
            .await;

        if verbose {
            println!("Found {:?} files", &objects.len());
        }

        objects
    }

    fn get_gcs_client(bucket_name: &String) -> Arc<dyn ObjectStore> {
        let builder = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket_name)
            .build();

        match builder {
            Ok(client) => Arc::new(client),
            Err(e) => panic!("Failed to create GCS client, {:?}", e),
        }
    }
}

impl StorageReader for GcsReader {
    async fn next(&mut self) -> ParquetResult<ParquetRecordBatchReader> {
        if self.file_list.is_empty() {
            return ParquetResult::Err(ParquetError::General("No more files".to_string()));
        }

        let file_path = self.file_list.remove(0);

        let client = Self::get_gcs_client(&self.bucket_name);

        if self.verbose {
            println!("Reading GCS object {:?}", &file_path);
        }

        let p = file_path.clone().into_os_string().into_string().unwrap();
        let object = client
            .get(&ObjectStorePath::parse(p).unwrap())
            .await
            .unwrap();
        let data = object.bytes().await.unwrap();

        if self.verbose {
            println!("Read GCS object {:?}", &file_path);
        }

        return ParquetRecordBatchReaderBuilder::try_new(data)
            .unwrap()
            .with_batch_size(8192)
            .build();
    }
}

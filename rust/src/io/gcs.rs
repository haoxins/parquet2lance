use arrow_array::RecordBatchReader;
use futures::stream::StreamExt;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::{path::Path as ObjectStorePath, ObjectStore};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::io::util::{
    get_bucket_name, get_object_path, get_object_prefix, is_parquet_object_path,
};
use crate::io::StorageReader;

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

        let client = get_gcs_client(&self.bucket_name);

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

        let result = ParquetRecordBatchReaderBuilder::try_new(data)
            .unwrap()
            .with_batch_size(8192)
            .build();

        match result {
            Ok(v) => Some(Box::new(v)),
            Err(e) => {
                if self.verbose {
                    println!("Failed to read GCS object {:?}, {}", &file_path, e);
                }
                None
            }
        }
    }
}

pub async fn get_file_list(p: &Path, verbose: bool) -> Vec<PathBuf> {
    let bucket_name = get_bucket_name(p).unwrap();

    if is_parquet_object_path(p) {
        return vec![get_object_path(p).unwrap()];
    }

    let prefix = get_object_prefix(p).unwrap();
    let client = get_gcs_client(&bucket_name);

    if verbose {
        println!("Getting GCS objects from {}", &prefix);
    }

    let objects = client.list(Some(&prefix)).await.unwrap();

    let objects = objects.map(|meta| {
        let meta = meta.unwrap();
        let p = meta.location.to_string();
        let ignored = !p.ends_with(".parquet") || meta.size == 0;

        if verbose {
            println!("Found file {}, ignored {}", &p, &ignored);
        }

        match ignored {
            true => PathBuf::from(""),
            false => PathBuf::from(p),
        }
    });

    let objects: Vec<PathBuf> = objects.collect().await;
    let objects: Vec<PathBuf> = objects
        .into_iter()
        .filter(|p| !p.to_str().unwrap().is_empty())
        .collect();

    if verbose {
        println!("Found {} files", &objects.len());
    }

    objects
}

fn get_gcs_client(bucket_name: &String) -> Arc<dyn ObjectStore> {
    let builder = GoogleCloudStorageBuilder::from_env()
        .with_bucket_name(bucket_name)
        .build();

    match builder {
        Ok(client) => Arc::new(client),
        Err(e) => panic!("Failed to create GCS client, {}", e),
    }
}

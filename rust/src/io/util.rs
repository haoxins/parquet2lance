use object_store::path::Path as ObjectStorePath;

use std::path::{Path, PathBuf};

pub fn get_bucket_name(p: &Path) -> Option<String> {
    let p = p.to_str().unwrap();
    let p = p.split("://").collect::<Vec<&str>>();

    if p.len() != 2 {
        return None;
    }

    Some(
        p[1].split('/')
            .collect::<Vec<&str>>()
            .first()
            .unwrap()
            .to_string(),
    )
}

pub fn get_object_path(p: &Path) -> Option<PathBuf> {
    let p = p.to_str().unwrap();
    let p = p.split("://").collect::<Vec<&str>>();

    if p.len() != 2 {
        return None;
    }

    let p = p[1].split('/').collect::<Vec<&str>>();

    if p.len() == 1 {
        return None;
    }

    let p = p[1..].join("/");

    Some(PathBuf::from(p))
}

pub fn get_object_prefix(p: &Path) -> Option<ObjectStorePath> {
    let p = p.to_str().unwrap();
    let p = p.split("://").collect::<Vec<&str>>();

    if p.len() != 2 {
        return None;
    }

    let dirs = p[1].split('/').collect::<Vec<&str>>();

    let size = dirs.len();

    if size < 2 {
        return None;
    }

    let prefix = if dirs.last().unwrap().contains('.') {
        dirs[1..size - 1].join("/")
    } else {
        dirs[1..].join("/")
    };

    Some(ObjectStorePath::from(prefix))
}

pub fn is_parquet_object_path(p: &Path) -> bool {
    let p = p.to_str().unwrap();
    p.ends_with(".parquet") && p.starts_with("gs://")
}

#[cfg(test)]
mod util_tests {
    use super::*;

    #[test]
    fn test_get_bucket_name() {
        assert_eq!(
            get_bucket_name(&PathBuf::from("gs://public-data/a.parquet")).unwrap(),
            "public-data"
        );
        assert_eq!(
            get_bucket_name(&PathBuf::from("gs://public-data")).unwrap(),
            "public-data"
        );
        assert_eq!(get_bucket_name(&PathBuf::from("/public-data")), None);
        assert_eq!(get_bucket_name(&PathBuf::from("")), None);
    }

    #[test]
    fn test_get_object_prefix() {
        assert_eq!(
            get_object_prefix(&PathBuf::from("gs://public-data/parquet/a.parquet")).unwrap(),
            ObjectStorePath::from("parquet")
        );
        assert_eq!(
            get_object_prefix(&PathBuf::from("gs://public-data/parquet/a")).unwrap(),
            ObjectStorePath::from("parquet/a")
        );
        assert_eq!(
            get_object_prefix(&PathBuf::from("gs://public-data/parquet")).unwrap(),
            ObjectStorePath::from("parquet")
        );
        assert_eq!(get_object_prefix(&PathBuf::from("gs://public-data")), None);
        assert_eq!(get_object_prefix(&PathBuf::from("/public-data")), None);
        assert_eq!(get_object_prefix(&PathBuf::from("")), None);
    }

    #[test]
    fn test_get_object_path() {
        assert_eq!(
            get_object_path(&PathBuf::from("gs://public-data/parquet/a.parquet")).unwrap(),
            PathBuf::from("parquet/a.parquet")
        );
        assert_eq!(
            get_object_path(&PathBuf::from("gs://public-data/parquet")).unwrap(),
            PathBuf::from("parquet")
        );
        assert_eq!(get_object_path(&PathBuf::from("gs://public-data")), None);
        assert_eq!(get_object_path(&PathBuf::from("/public-data")), None);
        assert_eq!(get_object_path(&PathBuf::from("")), None);
    }

    #[test]
    fn test_is_parquet_object_path() {
        assert!(is_parquet_object_path(&PathBuf::from(
            "gs://public-data/parquet/a.parquet"
        )));
        assert!(!is_parquet_object_path(&PathBuf::from(
            "gs://public-data/parquet"
        )));
        assert!(!is_parquet_object_path(&PathBuf::from(
            "/public-data/parquet/a.parquet"
        )));
        assert!(!is_parquet_object_path(&PathBuf::from(
            "/public-data/parquet"
        )));
        assert!(!is_parquet_object_path(&PathBuf::from(
            "/public-data/parquet/a.proto"
        )));
    }
}

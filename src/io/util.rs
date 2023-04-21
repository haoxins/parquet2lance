use std::path::PathBuf;

pub fn get_bucket_name(p: &PathBuf) -> Option<String> {
    let p = p.to_str().unwrap();
    let p = p.split("://").collect::<Vec<&str>>();

    if p.len() != 2 {
        return None;
    }

    Some(
        p[1].split("/")
            .collect::<Vec<&str>>()
            .first()
            .unwrap()
            .to_string(),
    )
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
}

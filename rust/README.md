## parquet2lance

[CI]: https://github.com/haoxins/parquet2lance/actions/workflows/rust.yaml
[CI Badge]: https://github.com/haoxins/parquet2lance/actions/workflows/rust.yaml/badge.svg
[crates.io]: https://crates.io/crates/parquet2lance
[crates.io badge]: https://img.shields.io/crates/v/parquet2lance.svg

[![CI Badge]][CI]
[![crates.io badge]][crates.io]

- Convert parquet files to [lance](https://github.com/eto-ai/lance)

```zsh
$ cargo install parquet2lance
$ parquet2lance --help
```

```
Convert parquet files to lance

Usage: parquet2lance [OPTIONS] --input <INPUT> --output-dir <OUTPUT_DIR>

Options:
  -i, --input <INPUT>            Input file or directory
  -o, --output-dir <OUTPUT_DIR>  Output directory
  -O, --overwrite                Overwrite output directory
      --verbose                  Print internal logs
  -h, --help                     Print help
  -V, --version                  Print version
```

- Working with GCS
  - Using `gcloud auth application-default login` to generate
    Application Default Credentials (ADC)

```zsh
parquet2lance \
  -i gs://cloud-samples-data/bigquery/us-states/ \
  -o test.lance -O

parquet2lance \
  -i gs://cloud-samples-data/bigquery/us-states/ \
  -o gs://your-bucket-name/bigquery/us-states/ -O

parquet2lance \
  -i gs://cloud-samples-data/bigquery/us-states/us-states.parquet \
  -o test.lance -O

parquet2lance \
  -i gs://cloud-samples-data/bigquery/us-states/us-states.parquet \
  -o gs://your-bucket-name/bigquery/us-states/us-states.lance -O
```

- Working with local FS

```zsh
gsutil cp gs://cloud-samples-data/bigquery/us-states/us-states.parquet test.parquet

parquet2lance -i test.parquet -o test.lance -O
```


- Convert parquet files to [lance](https://github.com/eto-ai/lance)

- Working with GCS
  - Using `gcloud auth application-default login` to generate
    Application Default Credentials (ADC)

```zsh
parquet2lance \
  -i gs://cloud-samples-data/bigquery/us-states/ \
  -o test.lance -O

parquet2lance \
  -i gs://cloud-samples-data/bigquery/us-states/us-states.parquet \
  -o test.lance -O
```

- From GCS to GCS

```zsh
parquet2lance \
  -i gs://cloud-samples-data/bigquery/us-states/ \
  -o gs://your-bucket-name/bigquery/us-states/

parquet2lance \
  -i gs://cloud-samples-data/bigquery/us-states/us-states.parquet \
  -o gs://your-bucket-name/bigquery/us-states/us-states.lance
```

- Working with local FS

```zsh
gsutil cp gs://cloud-samples-data/bigquery/us-states/us-states.parquet test.parquet

parquet2lance -i test.parquet -o test.lance -O
```


- Convert parquet files to [lance](https://github.com/lancedb/lance)

```py
from parquet2lance import p2l

p2l("gs://cloud-samples-data/bigquery/us-states/", "test.lance")
```

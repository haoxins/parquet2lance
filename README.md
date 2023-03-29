## parquet2lance

[CI]: https://github.com/haoxins/parquet2lance/actions/workflows/rust.yaml
[CI Badge]: https://github.com/haoxins/parquet2lance/actions/workflows/rust.yaml/badge.svg
[crates.io]: https://crates.io/crates/parquet2lance
[crates.io badge]: https://img.shields.io/crates/v/parquet2lance.svg

[![CI Badge]][CI]
[![crates.io badge]][crates.io]

Convert parquet to lance.

```zsh
$ cargo install parquet2lance

$ parquet2lance \
  --file ./testdata/alltypes_plain.parquet \
  --output-dir ./testdata/alltypes_plain
```

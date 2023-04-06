## parquet2lance

[CI]: https://github.com/haoxins/parquet2lance/actions/workflows/rust.yaml
[CI Badge]: https://github.com/haoxins/parquet2lance/actions/workflows/rust.yaml/badge.svg
[crates.io]: https://crates.io/crates/parquet2lance
[crates.io badge]: https://img.shields.io/crates/v/parquet2lance.svg

[![CI Badge]][CI]
[![crates.io badge]][crates.io]

Convert parquet files to [lance](https://github.com/eto-ai/lance)

```zsh
$ cargo install parquet2lance
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

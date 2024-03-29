## parquet2lance

[CI]: https://github.com/haoxins/parquet2lance/actions/workflows/rust.yaml
[CI Badge]: https://github.com/haoxins/parquet2lance/actions/workflows/rust.yaml/badge.svg
[crates.io]: https://crates.io/crates/parquet2lance
[crates.io badge]: https://img.shields.io/crates/v/parquet2lance.svg
[pypi.org]: https://pypi.org/project/parquet2lance/
[pypi.org badge]: https://img.shields.io/pypi/v/parquet2lance.svg

[![CI Badge]][CI]
[![crates.io badge]][crates.io]
[![pypi.org badge]][pypi.org]

- Convert parquet files to [lance](https://github.com/eto-ai/lance)
  - [Python bindings (PyO3)](./python)
  - [CLI and Rust core](./rust)

```zsh
$ cargo install parquet2lance
```

```zsh
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

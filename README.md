[Workflow Rust link]: https://github.com/haoxins/parquet2lance/actions/workflows/rust.yaml
[Workflow Rust badge]: https://github.com/haoxins/parquet2lance/actions/workflows/rust.yaml/badge.svg
[Workflow Python link]: https://github.com/haoxins/parquet2lance/actions/workflows/python.yaml
[Workflow Python badge]: https://github.com/haoxins/parquet2lance/actions/workflows/python.yaml/badge.svg
[Crate link]: https://crates.io/crates/parquet2lance
[Crate badge]: https://img.shields.io/crates/v/parquet2lance.svg
[PyPI link]: https://pypi.org/project/parquet2lance/
[PyPI badge]: https://img.shields.io/pypi/v/parquet2lance.svg

[![Workflow Rust badge]][Workflow Rust link]
[![Workflow Python badge]][Workflow Python link]
[![Crate badge]][Crate link]
[![PyPI badge]][PyPI link]

### parquet2lance

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

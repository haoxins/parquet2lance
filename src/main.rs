use arrow_array::RecordBatchReader;
use clap::Parser;
use lance::dataset::Dataset;
use lance::dataset::{WriteMode, WriteParams};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use std::fs::read_dir;
use std::fs::File;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    input: PathBuf,

    #[arg(short, long)]
    output_dir: PathBuf,

    #[arg(short, long)]
    overwrite: bool,

    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if args.input.is_file() {
        p2l(&vec![args.input], &args.output_dir, args.overwrite).await;
        return;
    }

    if args.input.is_dir() {
        let mut dir = read_dir(&args.input).unwrap();

        let mut paths = vec![];
        while let Some(Ok(entry)) = dir.next() {
            if entry.path().is_file() {
                paths.push(entry.path());
            }
        }

        p2l(&paths, &args.output_dir, args.overwrite).await;
        return;
    }

    panic!("Input path is neither a file nor a directory");
}

async fn p2l(paths: &Vec<PathBuf>, output_dir: &PathBuf, overwrite: bool) {
    let mut initialized = false;

    for p in paths {
        let file = File::open(p).unwrap();
        let mut reader: Box<dyn RecordBatchReader> = Box::new(
            ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .with_batch_size(8192)
                .build()
                .unwrap(),
        );

        let output_dir = output_dir.to_str().unwrap();

        let mut write_params = WriteParams::default();
        if !initialized {
            initialized = true;
            write_params.mode = if overwrite {
                WriteMode::Overwrite
            } else {
                WriteMode::Create
            };
        } else {
            write_params.mode = WriteMode::Append;
        }

        Dataset::write(&mut reader, output_dir, Some(write_params))
            .await
            .unwrap();
    }
}

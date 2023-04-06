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
    #[arg(short, long, help = "Input file or directory")]
    input: PathBuf,

    #[arg(short, long, help = "Output directory")]
    output_dir: PathBuf,

    #[arg(short = 'O', long, help = "Overwrite output directory")]
    overwrite: bool,

    #[arg(long, help = "Print internal logs")]
    verbose: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let files = get_input_files(&args.input);
    p2l(&files, &args.output_dir, args.overwrite).await;
}

async fn p2l(paths: &Vec<PathBuf>, output_dir: &PathBuf, overwrite: bool) {
    let mut initialized = false;

    for p in paths {
        let mut reader: Box<dyn RecordBatchReader> = read_file(p);

        let output_dir = output_dir.to_str().unwrap();

        let write_params = get_write_params(initialized, overwrite);

        Dataset::write(&mut reader, output_dir, Some(write_params))
            .await
            .unwrap();

        initialized = true;
    }
}

fn read_file(file_path: &PathBuf) -> Box<dyn RecordBatchReader> {
    let file = File::open(file_path).unwrap();

    Box::new(
        ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(8192)
            .build()
            .unwrap(),
    )
}

fn get_input_files(input: &PathBuf) -> Vec<PathBuf> {
    let mut files = vec![];

    if input.is_file() {
        files.push(input.clone());
        return files;
    }

    if input.is_dir() {
        let mut dir = read_dir(input).unwrap();
        while let Some(Ok(entry)) = dir.next() {
            // We don't handle directories yet
            if entry.path().is_file() {
                files.push(entry.path());
            }
        }

        return files;
    }

    panic!("Input path is neither a file nor a directory");
}

fn get_write_params(initialized: bool, overwrite: bool) -> WriteParams {
    let mut params = WriteParams::default();

    if !initialized {
        params.mode = if overwrite {
            WriteMode::Overwrite
        } else {
            WriteMode::Create
        };
        return params;
    }

    params.mode = WriteMode::Append;

    params
}

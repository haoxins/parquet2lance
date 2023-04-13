use arrow_array::RecordBatchReader;
use clap::Parser;
use lance::dataset::Dataset;
use lance::dataset::{WriteMode, WriteParams};

use std::path::PathBuf;

mod fs;

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

    let files = fs::get_input_files(&args.input);
    p2l(&files, &args.output_dir, args.overwrite).await;
}

async fn p2l(paths: &Vec<PathBuf>, output_dir: &PathBuf, overwrite: bool) {
    let mut initialized = false;

    for p in paths {
        let mut reader: Box<dyn RecordBatchReader> = fs::read_file(p);

        let output_dir = output_dir.to_str().unwrap();

        let write_params = get_write_params(initialized, overwrite);

        Dataset::write(&mut reader, output_dir, Some(write_params))
            .await
            .unwrap();

        initialized = true;
    }
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

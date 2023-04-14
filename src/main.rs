use clap::Parser;
use lance::dataset::Dataset;
use lance::dataset::{WriteMode, WriteParams};

use std::path::PathBuf;

use io::reader::Reader;
mod io;

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

    let reader = Reader::new(&args.input);
    p2l(reader, &args.output_dir, args.overwrite).await;
}

async fn p2l(mut reader: Reader, output_dir: &PathBuf, mut overwrite: bool) {
    let mut initialized = false;

    if !output_dir.exists() {
        overwrite = false;
    }

    while let Some(mut f) = reader.next() {
        let output_dir = output_dir.to_str().unwrap();

        let write_params = get_write_params(initialized, overwrite);

        Dataset::write(&mut f, output_dir, Some(write_params))
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

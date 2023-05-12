#![feature(async_fn_in_trait)]

use clap::Parser;

use std::path::PathBuf;

use io::Reader;
mod io;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short = 'i', long, help = "Input file or directory")]
    input: PathBuf,

    #[arg(short = 'o', long, help = "Output directory")]
    output_dir: PathBuf,

    #[arg(
        short = 'p',
        long,
        default_value = "1",
        help = "The parallelism of the conversion"
    )]
    parallelism: i32,

    #[arg(short = 'O', long, help = "Overwrite output directory")]
    overwrite: bool,

    #[arg(short = 'C', long, help = "Continue on failure (skip invalid files)")]
    continue_on_failure: bool,

    #[arg(short = 'v', long, help = "Print internal logs")]
    verbose: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let reader = Reader::new(&args.input, args.verbose).await;
    io::p2l(reader, &args.output_dir, args.overwrite).await;
}

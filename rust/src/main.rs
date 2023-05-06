#![feature(async_fn_in_trait)]

use clap::Parser;

use std::path::PathBuf;

use io::Reader;
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

    let reader = Reader::new(&args.input, args.verbose).await;
    io::p2l(reader, &args.output_dir, args.overwrite).await;
}

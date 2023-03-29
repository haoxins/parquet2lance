use arrow_array::RecordBatchReader;
use clap::Parser;
use lance::dataset::Dataset;
use lance::dataset::WriteParams;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use std::fs::File;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    file: std::path::PathBuf,

    #[arg(short, long)]
    output_dir: std::path::PathBuf,

    #[arg(short, long)]
    verbose: bool,
}

/* TODO
 *
 * -o <OUTPUT> Write output to a single file, OUTPUT.
 * --filelist <LIST> Read a list of files to operate on from LIST.
 * --output-dir-flat <DIR> Store processed files in DIR."
 * --output-dir-mirror <DIR> Store processed files in DIR, respecting original directory structure.
 */
#[tokio::main]
async fn main() {
    let args = Args::parse();

    let file = File::open(args.file).unwrap();

    let write_params = WriteParams::default();
    let mut reader: Box<dyn RecordBatchReader> = Box::new(
        ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(8192)
            .build()
            .unwrap(),
    );

    Dataset::write(
        &mut reader,
        args.output_dir.to_str().unwrap(),
        Some(write_params),
    )
    .await
    .unwrap();
}

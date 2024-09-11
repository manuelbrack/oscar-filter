use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(name = "oscar-filter")]
#[command(author = "Pedro Ortiz Suarez <pedro@commoncrawl.org>")]
#[command(version = "0.1.0")]
#[command(about = "Filters OSCAR's compressed jsonl files and converts them into parquet.", long_about = None)]
pub struct Cli {
    /// Folder containing the OSCAR compressed jsonl files
    #[arg(value_name = "INPUT FOLDER")]
    pub src: PathBuf,

    /// Destination folder for the parquet files
    #[arg(value_name = "DESTINATION FOLDER")]
    pub dst: PathBuf,

    /// Number of threads to use
    #[arg(short, long, default_value = "10", value_name = "NUMBER OF THREADS")]
    pub threads: usize,
}

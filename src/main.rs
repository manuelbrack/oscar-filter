use clap::Parser;

mod cleaner;
mod cli;
mod oscar;

#[tokio::main]

async fn main() {
    let cli = cli::Cli::parse();
    cleaner::clean_snapshot(&cli.src, cli.dst, cli.threads).await;
}

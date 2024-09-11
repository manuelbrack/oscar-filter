# oscar-filter

This repository contains the code to filter the OSCAR dataset, it filters OSCAR's compressed jsonl files and converts them into parquet.

## Usage

```text
➜ oscar-filter -h                                                                                
Filters OSCAR's compressed jsonl files and converts them into parquet.

Usage: oscar-filter [OPTIONS] <INPUT FOLDER> <DESTINATION FOLDER>

Arguments:
  <INPUT FOLDER>        Folder containing the OSCAR compressed jsonl files
  <DESTINATION FOLDER>  Destination folder for the parquet files

Options:
  -t, --threads <NUMBER OF THREADS>  Number of threads to use [default: 10]
  -h, --help                         Print help
  -V, --version                      Print version
```

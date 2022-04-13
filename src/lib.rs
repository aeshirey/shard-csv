//! A library for sharding delimited data from input streams according to a key selector.
//!
//! This crate allows you to easily stream data through a [`ShardedWriter`]. Each row
//! becomes associated with exactly one shard key and written out to a corresponding
//! output file. Multiple files can be streamed through the same `ShardedWriter`
//! provided they are of the same schema.
//!
//! # Current Limitations
//! * Input and output formats are limited to delimited (eg, CSV, TSV) formats, making
//!   use of the [`csv` crate](https://crates.io/crates/csv).
//!
//! # Using `csv`
//! Because `shard-csv` intimately deals with CSV data, it pulls in the `csv` crate and
//! re-exports it for callers as `shard_csv::csv`:
//!
//! ```
//! let mut csv_reader = shard_csv::csv::ReaderBuilder::new()
//!    .delimiter(b',')
//!    .has_headers(true)
//!    .from_path("foo.csv")
//!    .expect("Failed to create reader from file");
//! ```
//!
//! Several functions make use of the `csv::Reader` type, but you can write data without
//! it provided each row is converted to a `StringRecord`:
//!
//! ```
//! let data = vec![
//!     ["john", "smith", "123 main st"],
//!     ["jane", "doe", "999 anywhere"],
//! ];
//! let it = data.iter().map(|r| StringRecord::from(&r[..]));
//! shard_writer.process_iter(it).ok();
//! ```
//!
//! # Usage
//! A `ShardedWriter` must know about the header row. This can be accomplished by one of
//! three functions:
//! * [`ShardedWriterBuilder::new_without_header`] -- No header exists on input, and none
//!   will be written on output.
//! * [`ShardedWriterBuilder::new_with_header`] -- The input data contain a header which
//!   will be written to each output sharded file.
//! * [`ShardedWriterBuilder::new_from_csv_reader`] -- The presence or absence of a header
//!   is determined from a provided `csv::Reader` by way of its `.headers()` function.
//!
//! ```
//! let mut shard_writer = ShardedWriterBuilder::new_from_csv_reader(&mut csv_reader)
//!    .expect("Failed to create writer builder");
//! ```
//!
//! A writer must also know how to shard its data. You provide a closure that, for each
//! record in the file, identifies the shard to which the row belongs as a String. For
//! example, if the first column (index 0) is the identifier, you can get the value or
//! default to the empty string:
//!
//! ```
//! let mut shard_writer = ShardedWriterBuilder::new_from_csv_reader(&mut csv_reader)
//!    .expect("Failed to create writer builder");
//!    .with_key_selector(|rec| rec.get(0).unwrap_or("unknown").to_owned());
//! ```
//!
//! Finally, specify how output shard files are named by passing a closure to
//! `.with_output_shard_naming`. This closure is provided with the shard key and the
//! zero-based sequence number of the shard. That is, the first file written for any
//! given shard will have a sequence of zero; the second file will have a sequence of one,
//! and so on.
//!
//! Sequence numbers are generated based on [FileSplitting], which is specified by
//! `.with_output_splitting`.
//!
//! ```
//! let mut shard_writer = ShardedWriterBuilder::new_from_csv_reader(&mut csv_reader)
//!    .expect("Failed to create writer builder");
//!    .with_key_selector(|rec| rec.get(0).unwrap_or("unknown").to_owned());
//!    .with_output_shard_naming(|shard, seq| format!("{shard}-{seq}.csv"));
//! ```
//!
//! At this point, you can pass iterators of records (likely just the `csv_reader` itself)
//! to the writer:
//!
//! ```
//! shard_writer.process_csv(&mut csv_reader).ok();
//! ```
//!
//! # Additional options
//! ## Output Splitting
//! By default, all rows for a given shard will be written to the same file. If you want
//! to split the output into multiple files, provide details with `with_output_splitting`:
//!
//! ```
//! shard_writer = shard_writer.with_output_splitting(FileSplitting::SplitAfterRows(100));
//! ```
//!
//! ## File completion notification
//! When a shard is done being written -- either becuase the specified number of rows or
//! bytes were met and the writer is splitting to a new file or because the writer itself
//! is being dropped and cleaning up open file handles -- you can be notified of the file's
//! completion. The completed file path and the associated shard key will be provided.
//!
//! ```
//! shard_writer = shard_writer.on_file_completion(|path, key| {
//!     println!("Output file '{}' for key '{key}' is complete", path.display());
//! });
//! ```
//!
//! ## Alternate file creation
//! By default, when a new shard file is created, a `BufWriter<File>` is created
//! automatically. If you want to create your own file (eg, with a GZip stream writer),
//! override this with `.on_create_file`, which returns a `Box<dyn Write>` on success:
//! ```
//! shard_writer = shard_writer.on_create_file(|path| {
//!     let f = std::fs::File::create(path)?;
//!     let gz = flate2::write::GzEncoder::new(f, flate2::Compression::fast());
//!     let buf = BufWriter::new(gz);
//!     Ok(Box::new(buf))
//! });
//! ```
mod shard;
mod sharded_writer;

pub use csv;
pub use sharded_writer::*;

/// Defines how output files will be split
#[derive(Clone, Copy, Debug)]
pub enum FileSplitting {
    /// Output files won't be split
    NoSplit,

    /// Output files will be split after at least some number of rows are written
    SplitAfterRows(usize),

    /// Output files will be split after at least some number of bytes are written
    SplitAfterBytes(usize),
}

impl Default for FileSplitting {
    fn default() -> Self {
        FileSplitting::NoSplit
    }
}

#[derive(Debug)]
pub enum Error {
    Csv(csv::Error),
    IO(std::io::Error),
}

impl From<csv::Error> for Error {
    fn from(e: csv::Error) -> Self {
        Error::Csv(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IO(e)
    }
}

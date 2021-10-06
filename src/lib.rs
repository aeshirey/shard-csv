mod shard;
mod sharded_writer;
pub use sharded_writer::ShardedWriter;


/// Defines how input headers should be handled
#[derive(Clone, Copy, Debug)]
pub enum HeaderHandling {
    /// The input file does not have a header
    NoHeader,

    /// The input file has a header, but it should not be retained on output
    RemoveHeader,

    /// The input file has a header, and it should be retained on output
    KeepHeader,
}

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

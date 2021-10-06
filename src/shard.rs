use crate::{Error, FileSplitting};
use csv::{StringRecord, Writer};
use std::{
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

/// Represents an individual file written out.
struct ShardFile {
    path: PathBuf,
    key: String,
    writer: Writer<Box<dyn Write>>,
    written: usize,
    splitting: FileSplitting,
}

impl ShardFile {
    /// Writes the `record` to this open file.
    ///
    /// This function bubbles up underdlying CSV writer errors on failure.
    /// On success, this returns true if and only if the file should be closed (we've met the conditions to split).
    fn write_record(&mut self, record: &StringRecord) -> Result<bool, Error> {
        self.writer.write_record(record)?;

        Ok(match self.splitting {
            FileSplitting::NoSplit => false,
            FileSplitting::SplitAfterRows(rows) => {
                self.written += 1;
                self.written >= rows
            }
            FileSplitting::SplitAfterBytes(bytes) => {
                self.written += record.as_byte_record().as_slice().len();
                self.written >= bytes
            }
        })
    }
}

/// A logical sharded subset of the input data.
pub(crate) struct Shard {
    directory: PathBuf,
    key: String,
    sequence: usize,
    extension: String,

    splitting: FileSplitting,
    current_file: Option<ShardFile>,
    header_record: Option<StringRecord>,

    create_file: Option<fn(&Path) -> std::io::Result<Box<dyn Write>>>,
    on_completion: Option<fn(&Path, &str)>,
}

impl Shard {
    fn path(&self) -> std::path::PathBuf {
        Path::new(&self.directory).join(&format!(
            "{}-{}.{}",
            self.key, self.sequence, self.extension
        ))
    }

    pub fn new(
        splitting: FileSplitting,
        directory: &Path,
        key: String,

        extension: &str,
        header: &Option<StringRecord>,
        create_file: Option<fn(&Path) -> std::io::Result<Box<dyn Write>>>,
        on_completion: Option<fn(&Path, &str)>,
    ) -> Self {
        Self {
            splitting,
            current_file: None,
            header_record: header.clone(),
            on_completion,
            directory: directory.to_owned(),
            key,
            sequence: 0,
            create_file,
            extension: extension.to_owned(),
        }
    }

    pub fn write_record(&mut self, record: StringRecord) -> Result<(), crate::Error> {
        match self.current_file.as_mut() {
            Some(sf) => {
                // File is already in-progress
                if sf.write_record(&record)? {
                    // And we should wrap this one up.
                    if let Some(s) = self.current_file.take() {
                        if let Some(callback) = self.on_completion {
                            let ShardFile {
                                path, key, writer, ..
                            } = s;
                            drop(writer);
                            callback(&path, &key);
                        }
                    }
                }
            }
            None => {
                // Start a new file
                let writer = match self.create_file {
                    Some(f) => (f)(&self.path())?,
                    None => {
                        let writer = std::fs::File::create(self.path())?;
                        let buf = BufWriter::new(writer);
                        Box::new(buf)
                    }
                };

                let mut writer = Writer::from_writer(writer);

                if let Some(h) = &self.header_record {
                    writer.write_record(h)?;
                }

                let mut shard_file = ShardFile {
                    path: self.path(),
                    key: self.key.to_owned(),
                    writer,
                    written: 0,
                    splitting: self.splitting,
                };

                self.sequence += 1;

                // This seems an unnecessary step -- but if we only want to write one row or very few bytes to
                // a stream, we'll preserve this check.
                if !shard_file.write_record(&record)? {
                    self.current_file = Some(shard_file);
                }
            }
        }

        Ok(())
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        if let Some(ShardFile {
            path, key, writer, ..
        }) = self.current_file.take()
        {
            if let Some(callback) = self.on_completion {
                // Explicitly drop the writer so the file gets flushed an the handle closed.
                drop(writer);

                // *Then* call back to the client
                callback(&path, &key);
            }
        }
    }
}

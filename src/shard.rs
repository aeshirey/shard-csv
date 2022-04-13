use crate::{Error, FileSplitting};
use csv::{StringRecord, Writer};
use std::{
    io::Write,
    path::{Path, PathBuf},
    rc::Rc,
};

pub(crate) type CreateFileWriter = fn(&Path) -> std::io::Result<Box<dyn Write>>;

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
pub(crate) struct Shard<FNameFile>
where
    FNameFile: Fn(&str, usize) -> String,
{
    /// The shard value
    key: String,

    /// The current, zero-based number identifying how many files have been
    /// written for this shard
    sequence: usize,

    /// How output files will be split up
    splitting: FileSplitting,

    /// A reference to the [ShardFile], if one is open, for outputting rows.
    current_file: Option<ShardFile>,

    /// The optional header row to be written to each sharded file.
    header_record: Option<StringRecord>,

    /// A function that creates each output shard.
    ///
    /// By default, this will create a buffered text writer, but if you want
    /// to gzip output, for example, this function overrides that behavior.
    create_file_writer: CreateFileWriter,

    /// A function to be called when each sharded file is complete.
    ///
    /// A file is complete when the Shard gets dropped, which is either when
    /// the [ShardedWriter] is itself dropped or when a new [ShardFile] is
    /// created for file splitting.
    on_file_completion: Option<fn(&Path, &str)>,

    /// A function that defines how intermediate shard files are named.
    ///
    /// By default, files are named as `{shard}-{sequence}.{extension}`. For
    /// example, "washington-7.csv" might be created when sharding on US
    /// state names.
    ///
    /// You may override this with [`.with_output_shard_naming`]:
    /// 
    /// ```
    /// let mut shard_writer = ShardedWriterBuilder::new_from_csv_reader(&mut csv_reader)
    ///    .expect("Failed to create writer builder");
    ///    .with_key_selector(|rec| rec.get(0).unwrap_or("unknown").to_owned());
    ///    .with_output_shard_naming(|shard, seq| format!("{shard}-{seq}.csv"));
    /// ```
    create_output_filename: Rc<FNameFile>,
}

impl<FNameFile> Shard<FNameFile>
where
    FNameFile: Fn(&str, usize) -> String,
{
    fn path(&self) -> std::path::PathBuf {
        (self.create_output_filename)(&self.key, self.sequence).into()
    }

    pub fn new(
        splitting: FileSplitting,
        key: String,

        header_record: Option<StringRecord>,
        create_file_writer: CreateFileWriter,
        create_output_filename: Rc<FNameFile>,
        on_file_completion: Option<fn(&Path, &str)>,
    ) -> Self {
        Self {
            splitting,
            current_file: None,
            header_record,
            on_file_completion,
            key,
            sequence: 0,
            create_output_filename,
            create_file_writer,
        }
    }

    pub fn write_record(&mut self, record: &StringRecord) -> Result<(), crate::Error> {
        match self.current_file.as_mut() {
            Some(sf) => {
                // File is already in-progress
                if sf.write_record(record)? {
                    // And we should wrap this one up.
                    if let Some(s) = self.current_file.take() {
                        if let Some(callback) = &self.on_file_completion {
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
                let writer = (self.create_file_writer)(&self.path())?;
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
                if !shard_file.write_record(record)? {
                    self.current_file = Some(shard_file);
                }
            }
        }

        Ok(())
    }
}

impl<FNameFile> Drop for Shard<FNameFile>
where
    FNameFile: Fn(&str, usize) -> String,
{
    fn drop(&mut self) {
        if let Some(ShardFile {
            path, key, writer, ..
        }) = self.current_file.take()
        {
            if let Some(callback) = &self.on_file_completion {
                // Explicitly drop the writer so the file gets flushed and the handle closed.
                drop(writer);

                // *Then* call back to the client because now the file is definitely dropped.
                callback(&path, &key);
            }
        }
    }
}

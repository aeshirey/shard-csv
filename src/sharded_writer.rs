use crate::{shard::Shard, Error, FileSplitting, HeaderHandling};
use csv::StringRecord;
use std::{
    collections::{hash_map::Entry, HashMap},
    path::{Path, PathBuf},
};

pub struct ShardedWriter {
    /// How the header row should be handled
    header_handling: HeaderHandling,

    /// How the input file should be split
    file_splitting: FileSplitting,

    /// The field delimiter; default is '\t'
    delimiter: u8,

    /// A closure that accepts a CSV row and returns a String identifying which shard it belongs to.
    key_selector: fn(&StringRecord) -> String,

    extension: Option<String>,

    on_completion: Option<fn(&Path, &str)>,

    output_directory: Option<PathBuf>,

    handles: HashMap<String, Shard>,
}

impl std::fmt::Debug for ShardedWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedWriter")
            .field("header", &self.header_handling)
            .field("split", &self.file_splitting)
            .field("delimiter", &self.delimiter)
            .field("extension", &self.extension)
            .finish()
    }
}

impl ShardedWriter {
    pub fn new(key_selector: fn(&StringRecord) -> String) -> Self {
        Self {
            file_splitting: FileSplitting::NoSplit,
            header_handling: HeaderHandling::NoHeader,
            delimiter: b'\t',
            key_selector,
            extension: None,
            on_completion: None,
            handles: HashMap::new(),
            output_directory: None,
        }
    }

    /// Specifies how to handle the header row
    pub fn header_handling(mut self, header: HeaderHandling) -> Self {
        self.header_handling = header;
        self
    }

    /// Specifies how output files should be split.
    pub fn split_output(mut self, splitting: FileSplitting) -> Self {
        self.file_splitting = splitting;
        self
    }

    /// Explicitly sets the output file extension to use. If no value is set,
    /// one will be inferred from the delimiter, either the default `\t` or
    /// one provided by calling [delimiter].
    pub fn output_extension<T>(mut self, extension: T) -> Self
    where
        T: Into<String>,
    {
        self.extension = Some(extension.into());
        self
    }

    /// Sets the field delimiter to be used for outptu files; default is '\t'.
    pub fn delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;

        if self.extension.is_none() {
            // No extension is set -- infer it from the filename
            self.extension = Some(match delimiter {
                b'\t' => "tsv".to_string(),
                b',' => "csv".to_string(),
                _ => "csv".to_string(),
            });
        }

        self
    }

    /// Sets an optional function that will be called when individual files are completed, either
    /// because they have been split by the number of rows or bytes or because processing is
    /// complete and the values are being dropped.
    pub fn on_completion(mut self, f: fn(&Path, &str)) -> Self {
        self.on_completion = Some(f);
        self
    }

    /// Explicitly sets the directory into which output files will be written.
    ///
    /// If this isn't called, the output directory will be derived from the first filename
    /// passed to [process_file].
    pub fn output_to<T>(mut self, directory: T) -> Self
    where
        T: Into<PathBuf>,
    {
        self.output_directory = Some(directory.into());
        self
    }

    /// Processes the input `filename`, creating output files according to the specified key
    /// selector.
    ///
    /// This function will fail if the output directory or an output file can't be created or if a
    /// row can't be written. It can also fail if it is called multiple times with files that have
    /// different column counts.
    ///
    /// On success, the number of records written is returned.
    pub fn process_file(&mut self, filename: &str) -> Result<usize, Error> {
        let directory = match &self.output_directory {
            Some(d) => d.clone(),
            None => std::path::Path::new(filename).with_extension(""),
        };

        if !directory.exists() {
            std::fs::create_dir(&directory)?;
        }

        if self.extension.is_none() {
            self.extension = Some(match self.delimiter {
                b'\t' => "tsv".to_string(),
                b',' => "csv".to_string(),
                _ => "csv".to_string(),
            });
        }

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(matches!(
                self.header_handling,
                HeaderHandling::RemoveHeader | HeaderHandling::KeepHeader
            ))
            .from_path(filename)?;

        let header_record = match self.header_handling {
            HeaderHandling::NoHeader => None,
            HeaderHandling::RemoveHeader => {
                reader.headers()?;
                None
            }
            HeaderHandling::KeepHeader => Some(reader.headers()?.clone()),
        };

        let mut records_written = 0;
        for record in reader.records() {
            let record = match record {
                Ok(r) => r,
                Err(_) => continue, // skip invalid records?
            };

            let key = (self.key_selector)(&record);

            match self.handles.entry(key.clone()) {
                Entry::Occupied(mut e) => {
                    e.get_mut().write_record(record)?;
                }
                Entry::Vacant(e) => {
                    let mut shard = Shard::new(
                        self.file_splitting,
                        &directory,
                        key,
                        &self.extension.as_ref().unwrap(),
                        &header_record,
                        self.on_completion,
                    );

                    shard.write_record(record)?;

                    e.insert(shard);
                }
            };

            records_written += 1;
        }

        Ok(records_written)
    }
}

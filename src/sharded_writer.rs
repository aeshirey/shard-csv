use crate::{
    shard::{self, Shard},
    Error, FileSplitting,
};
use csv::StringRecord;
use std::{
    collections::{hash_map::Entry, HashMap},
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub struct ShardedWriter {
    /// How the input file should be split
    output_splitting: FileSplitting,

    /// The field delimiter; default is ','
    delimiter: u8,

    /// A closure that accepts a CSV row and returns a String identifying which shard it belongs to.
    key_selector: fn(&StringRecord) -> String,

    header: Option<StringRecord>,

    /// A function that will be called when an intermediate file is completed
    on_file_completion: Option<fn(&Path, &str)>,

    output_directory: PathBuf,

    create_output_filename: fn(shard: &str, seq: usize) -> String,

    /// A closure that creates a writer for a requested output file path
    on_create_file: crate::shard::CreateFileFunction,

    handles: HashMap<String, Shard>,
}

impl std::fmt::Debug for ShardedWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedWriter")
            .field("output_splitting", &self.output_splitting)
            .field("delimiter", &self.delimiter)
            .finish()
    }
}

impl ShardedWriter {
    /// Creates a new writer.
    ///
    /// You must specify the directory into which the output will be written, a function
    /// that extracts the shard key from a csv [StringRecord], and how output files will
    /// be named. The file naming function accepts the shard key and a zero-based number
    /// indicating how many files have been created for this shard.
    ///
    /// This function can return an error if the output directory can't be created.
    ///
    /// ```
    /// let writer = ShardedWriter::new(
    ///     "./foo-sharded/",
    ///     |record| record.get(7).unwrap_or("_unknown").to_string(),
    ///     |shard, seq| format!("{}-file{}.csv", shard, seq)
    /// )?;
    /// ```
    pub fn new<T>(
        output_directory: T,
        key_selector: fn(&StringRecord) -> String,
        name_file: fn(&str, usize) -> String,
    ) -> Result<Self, Error>
    where
        T: Into<PathBuf>,
    {
        let output_directory = output_directory.into();
        if !output_directory.exists() {
            println!("create dir: {:?}", output_directory);
            std::fs::create_dir(&output_directory)?;
        }

        Ok(Self {
            output_splitting: FileSplitting::NoSplit,
            delimiter: b',',
            key_selector,
            on_file_completion: None,
            handles: HashMap::new(),
            on_create_file: shard::default_on_create_file,
            output_directory,
            create_output_filename: name_file,
            header: None,
        })
    }

    /// Specifies that this writer will emit a header row on output as specified by `header`.
    pub fn with_header<T>(mut self, header: T) -> Self
    where
        T: Into<StringRecord>,
    {
        self.header = Some(header.into());
        self
    }

    /// Specifies that this writer will emit a header row that comes from the `reader`'s header.
    pub fn with_header_from<T>(mut self, reader: &mut csv::Reader<T>) -> Result<Self, Error>
    where
        T: Read,
    {
        self.header = Some(reader.headers()?.clone());
        Ok(self)
    }

    /// Specifies how output files should be split.
    pub fn with_output_splitting(mut self, output_splitting: FileSplitting) -> Self {
        self.output_splitting = output_splitting;
        self
    }

    /// Sets the field delimiter to be used for output files; default is '\t'.
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Sets an optional function that will be called when individual files are completed, either
    /// because they have been split by the number of rows or bytes or because processing is
    /// complete and the values are being dropped.
    pub fn on_file_completion(mut self, f: fn(path: &Path, shard_key: &str)) -> Self {
        self.on_file_completion = Some(f);
        self
    }

    /// Takes a closure that specifies how to create output files.
    ///
    /// The closure provides the [Path] of the output file to be created.
    pub fn on_create_file(mut self, f: fn(&Path) -> std::io::Result<Box<dyn Write>>) -> Self {
        self.on_create_file = f;
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
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(self.header.is_some())
            .from_path(filename)?;

        let records = reader.records().filter_map(|r| r.ok());
        //let records = reader.records().filter_map(|r| r.ok());
        self.process_iter(records)
    }

    /// Processes the input reader, creating output files according to the specified key
    /// selector.
    ///
    /// This function will fail if the output directory or an output file can't be created or if a
    /// row can't be written. It can also fail if it is called multiple times with files that have
    /// different column counts.
    ///
    /// On success, the number of records written is returned.
    pub fn process_csv<T: std::io::Read>(
        &mut self,
        reader: &mut csv::Reader<T>,
    ) -> Result<usize, Error> {
        let records = reader.records().filter_map(|r| r.ok());

        self.process_iter(records)
    }

    pub fn process_reader(&mut self, reader: impl std::io::Read) -> Result<usize, Error> {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .has_headers(self.header.is_some())
            .from_reader(reader);

        let records = reader.records().filter_map(|r| r.ok());

        self.process_iter(records)
    }

    fn process_iter<T>(&mut self, records: T) -> Result<usize, Error>
    where
        T: IntoIterator<Item = StringRecord>,
    {
        if !self.output_directory.exists() {
            std::fs::create_dir(&self.output_directory)?;
        }

        let mut records_written = 0;
        for record in records {
            let key = (self.key_selector)(&record);

            match self.handles.entry(key.clone()) {
                Entry::Occupied(mut e) => {
                    e.get_mut().write_record(&record)?;
                }
                Entry::Vacant(e) => {
                    let mut shard = Shard::new(
                        self.output_splitting,
                        &self.output_directory,
                        key,
                        self.header.clone(),
                        self.on_create_file,
                        self.create_output_filename,
                        self.on_file_completion,
                    );

                    shard.write_record(&record)?;

                    e.insert(shard);
                }
            };

            records_written += 1;
        }

        Ok(records_written)
    }

    /// Checks if `key` has been seen in the processed data
    pub fn has_seen_shard_key(&self, key: &str) -> bool {
        self.handles.contains_key(key)
    }
}

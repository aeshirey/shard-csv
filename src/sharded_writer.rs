use crate::{shard, Error, FileSplitting};
use csv::StringRecord;
use std::{
    collections::{hash_map::Entry, HashMap},
    io::{BufWriter, Write},
    path::Path,
    rc::Rc,
};

pub struct ShardedWriterBuilder {
    header: Option<StringRecord>,
}

impl ShardedWriterBuilder {
    /// Start creating a sharded writer for data that don't have a header.
    pub fn new_without_header() -> Self {
        ShardedWriterBuilder { header: None }
    }

    /// Start creating a sharded writer for data with the specified `header`.
    pub fn new_with_header<T>(header: T) -> Self
    where
        T: Into<StringRecord>,
    {
        ShardedWriterBuilder {
            header: Some(header.into()),
        }
    }

    /// Start creating a sharded writer from the specified [`csv::Reader`]
    ///
    /// The reader's header settings will be copied over to the sharded writer.
    pub fn new_from_csv_reader<T>(csv: &mut csv::Reader<T>) -> Result<Self, Error>
    where
        T: std::io::Read,
    {
        let header = if csv.has_headers() {
            Some(csv.headers()?.clone())
        } else {
            None
        };

        Ok(Self { header })
    }

    /// Specifies how the input will be sharded.
    ///
    /// Given a row of input, the key selector determines which shard the record belongs in.
    pub fn with_key_selector<FKey>(self, key_selector: FKey) -> ShardedWriterWithKey<FKey>
    where
        FKey: Fn(&StringRecord) -> String,
    {
        ShardedWriterWithKey {
            header: self.header,
            key_selector,
        }
    }
}

pub struct ShardedWriterWithKey<FKey> {
    header: Option<StringRecord>,
    key_selector: FKey,
}

impl<FKey> ShardedWriterWithKey<FKey>
where
    FKey: Fn(&StringRecord) -> String,
{
    /// Specifies how output shard files will be named.
    ///
    /// The specified function will be called with the key value (derived from the `key_selector`
    /// passed to [`ShardedWriterBuilder::with_key_selector`]) and the current sequence number,
    /// which is a zero-based number identifying how many files have been written for this shard.
    pub fn with_output_shard_naming<FNameFile>(
        self,
        create_output_filename: FNameFile,
    ) -> ShardedWriter<FKey, FNameFile>
    where
        FNameFile: Fn(&str, usize) -> String,
    {
        let ShardedWriterWithKey {
            header,
            key_selector,
        } = self;

        ShardedWriter {
            header_record: header,
            key_selector,
            output_splitting: FileSplitting::NoSplit,
            output_delimiter: b',',
            on_file_completion: None,
            create_file_writer: default_create_file_writer,
            create_output_filename: Rc::new(create_output_filename),
            handles: HashMap::new(),
        }
    }
}

pub struct ShardedWriter<FKey, FNameFile>
where
    FNameFile: Fn(&str, usize) -> String,
{
    /// How the input file should be split
    output_splitting: FileSplitting,

    /// The field delimiter; default is ','
    output_delimiter: u8,

    /// A closure that accepts a CSV row and returns a String identifying which shard it belongs to.
    key_selector: FKey,

    /// An optional header record that will be written to every output file.
    header_record: Option<StringRecord>,

    /// A function that will be called when an intermediate file is completed
    on_file_completion: Option<fn(&Path, &str)>,

    create_output_filename: Rc<FNameFile>,

    /// A function that creates a writer for a requested output file path
    create_file_writer: crate::shard::CreateFileWriter,

    /// A mapping of shard keys to the shards that output to files
    handles: HashMap<String, shard::Shard<FNameFile>>,
}

impl<FKey, FNameFile> std::fmt::Debug for ShardedWriter<FKey, FNameFile>
where
    FNameFile: Fn(&str, usize) -> String,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedWriter")
            .field("output_splitting", &self.output_splitting)
            .field("delimiter", &self.output_delimiter)
            .finish()
    }
}

impl<FKey, FNameFile> ShardedWriter<FKey, FNameFile>
where
    FKey: Fn(&StringRecord) -> String,
    FNameFile: Fn(&str, usize) -> String,
{
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

    /// Specifies when sharded output files should be split.
    pub fn with_output_splitting(mut self, output_splitting: FileSplitting) -> Self {
        self.output_splitting = output_splitting;
        self
    }

    /// Sets the field delimiter to be used for output files. Default is ','.
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.output_delimiter = delimiter;
        self
    }

    /// Sets an optional function that will be called when individual files are completed, either
    /// because they have been split by the number of rows or bytes or because processing is
    /// complete and the values are being dropped.
    pub fn on_file_completion(mut self, f: fn(&Path, &str)) -> Self {
        self.on_file_completion = Some(f);
        self
    }

    /// Takes a closure that specifies how to create output files.
    ///
    /// The closure provides the [Path] of the output file to be created. If you don't
    /// provide your own way to create output files, the default implementation will simply create
    /// a new [BufWriter] for the output file, which is the same as:
    ///
    /// ```
    /// my_sharded_writer.on_create_file(|path| Ok(BufWriter::new(File::create(path)?)));
    /// ```
    ///
    /// This function may be useful if, for example, you want to inject gzip compression into the
    /// output writer.
    pub fn on_create_file(mut self, f: fn(&Path) -> std::io::Result<Box<dyn Write>>) -> Self {
        self.create_file_writer = f;
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
            .delimiter(self.output_delimiter)
            .has_headers(self.header_record.is_some())
            .from_path(filename)?;

        let records = reader.records().filter_map(|r| r.ok());
        self.process_iter(records)
    }

    /// Processes the input reader, creating output files as appropriate.
    ///
    /// This function will fail if the output directory or an output file can't be created or if a
    /// row can't be written. It can also fail if it is called multiple times with files that have
    /// different column counts.
    ///
    /// On success, the number of records written is returned.
    pub fn process_csv<T: std::io::Read>(
        &mut self,
        csv_reader: &mut csv::Reader<T>,
    ) -> Result<usize, Error> {
        let records = csv_reader.records().filter_map(|r| r.ok());

        self.process_iter(records)
    }

    /// Processes an iterator of [std::io::Read], creating output files as appropriate.
    pub fn process_reader(&mut self, reader: impl std::io::Read) -> Result<usize, Error> {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(self.output_delimiter)
            .has_headers(self.header_record.is_some())
            .from_reader(reader);

        let records = reader.records().filter_map(|r| r.ok());

        self.process_iter(records)
    }

    /// Iterates over every record, calculating the shard key for each, getting or creating the shard file,
    /// and writing the record.
    pub fn process_iter<T>(&mut self, records: T) -> Result<usize, Error>
    where
        T: IntoIterator<Item = StringRecord>,
    {
        let mut records_written = 0;
        for record in records {
            let key = (self.key_selector)(&record);

            match self.handles.entry(key.clone()) {
                Entry::Occupied(mut e) => {
                    e.get_mut().write_record(&record)?;
                }
                Entry::Vacant(e) => {
                    let header_record = self.header_record.clone();
                    let create_output_filename = self.create_output_filename.clone();
                    let mut shard = shard::Shard::new(
                        self.output_splitting,
                        key,
                        header_record,
                        self.create_file_writer,
                        create_output_filename,
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

    /// Checks if `key` has been seen in the processed data.
    pub fn is_shard_key_seen(&self, key: &str) -> bool {
        self.handles.contains_key(key)
    }

    /// Returns a vec of all keys that have been seen.
    pub fn shard_keys_seen(&self) -> Vec<String> {
        self.handles.keys().cloned().collect()
    }
}

/// The standard approach to creating a file writer -- create and buffer it.
///
/// To do something different (such as gzipping output), [ShardedWriter::on_create_file]
/// is passed an alternate function with this signature.
fn default_create_file_writer(path: &Path) -> std::io::Result<Box<dyn Write>> {
    let writer = std::fs::File::create(path)?;
    let buf = BufWriter::new(writer);
    Ok(Box::new(buf))
}

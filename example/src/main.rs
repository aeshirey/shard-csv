use shard_csv::*;

fn main() {
    // Create a CSV reader for our file, which is of the format "name,city,language".
    // By default, `csv` uses a comma delimiter and assumes a header row, so we don't
    // have to specify them here
    let mut reader = shard_csv::csv::ReaderBuilder::new()
        .from_path("input_data.csv")
        .expect("Failed to create reader from file");

    // Create our writer, sharded by the language spoken
    let mut writer = ShardedWriterBuilder::new_from_csv_reader(&mut reader)
        .expect("Failed to create writer")
        .with_key_selector(|row| row.get(2).unwrap_or("language_unknown").to_string())
        .with_output_shard_naming(|key, seq| format!("data_lang={}.part{}.csv", key, seq))
        .with_output_splitting(FileSplitting::SplitAfterBytes(1024 * 1024))
        .on_file_completion(|path, key| {
            println!("The file {} is now ready for shard {}", path.display(), key);
            // Upload the file to our remote server or something.
        });

    writer.process_csv(&mut reader).ok();
}

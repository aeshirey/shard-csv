# shard-csv
`shard-csv` is a crate to split input CSV files into output shards according to some key selector. Use it when you have some large dataset that you want to split out with more control than, say, GNU split.

## Usage
Include it in your Cargo.toml with: `shard-csv = "0.1.0"`.

Sample usage first entails creating a CSV reader. Note that `shard-csv` depends heavily upon the [`csv` crate](https://crates.io/crates/csv), which it, in fact, uses and re-exports:

```rust
let mut reader = shard_csv::csv::ReaderBuilder::new()
    .from_path("input_data.csv")
    .expect("Failed to create reader from file");
```

Then you can create a sharded CSV writer that:
* Knows how to identify which shard each row belongs to,
* Can write each shard into a single file or multiple files, split on number of rows or size in bytes,
* Can create output streams arbitrarily (eg, gzipped),
* Notifies you when a stream is complete

```rust
let mut writer = ShardedWriterBuilder::new_from_csv_reader(&mut reader)
    .expect("Failed to create writer")
    .with_key_selector(|row| row.get(2).unwrap_or("unknown").to_string())
    .with_output_shard_naming(|key, seq| format!("data.part{}.csv", key, seq))
    .with_output_splitting(FileSplitting::SplitAfterBytes(1024 * 1024))
    .on_file_completion(|path, key| {
        println!("The file {} is now ready for shard {}", path.display(), key);
        // Do something more with the completed file if you want.
    });

writer.process_csv(&mut reader).ok();
```

# b2

An implementation of [Bitcask](https://riak.com/assets/bitcask-intro.pdf) as a Rust library.

## what can it do

Bitcask presents a very simple KV store API, so that is what this does, too.
See the Bitcask paper to understand why Bitcask's particular conception of a KV store is unique and interesting.

This is the public API:

```rust
pub async fn new(db_directory: &Path, options: Options) -> Result<Self>
pub async fn get<V: Serialize + DeserializeOwned + Send>(&self, key: &K) -> Result<Option<V>>
pub async fn insert<V: Serialize + DeserializeOwned + Send>(&self, k: K, v: V) -> Result<()>
pub async fn remove(&self, k: K) -> Result<()>
pub async fn contains_key(&self, k: &K) -> bool
pub async fn merge(&self) -> Result<()>
pub async fn flush(&self) -> Result<()>
```

For a given database, keys must all be the same type (i.e., all `String`, or whatever other type can implement `Serialize` and `DeserializeOwned`), but values can vary arbitrarily, again as long as they can be serialized and deserialized. This could change at some point, who knows. See the tests for examples of this.

In terms of concurrency, right now this uses a `tokio::sync::RwLock`, so there can be: `(N readers) XOR (1 writer)`. Given Bitcask's model, it is possible to relax this so that there can be `(N readers) AND (1 writer)`, and I might do that in the future.

By default it flushes every write to disk. This is slow, but leads to predictable read-after-write semantics. You can relax this (and increase write throughput) by changing an option.

## is it any good? should I use it?

Probably not! From what I can tell, it is API complete with respect to the Bitcask paper. This does not mean it functions correctly. It is undertested. It uses a simple `tokio::sync::RwLock` internally so its concurrency story is weak. There are probably a bunch of other problems with it. Nonetheless, it is a tiny amount of code in comparison to other database systems, you can probably actually understand what this does just by reading the source.

## why

I have known about Bitcask for a while, and I wanted to learn it by building a working implementation.

## todo

- [ ] better testing (in general)
- [ ] better testing (around merging, specifically)
- [ ] allow concurrent reading and writing (relax RwLock)
- [ ] clean up merging code
- [ ] clean up datamodel around records/entrypointers/mergepointers
- [ ] more research into how async drop interacts with disk writes/buffer flushes
- [x] investigate a better, less ambiguous tombstone value
- [x] move more of write_insert and write_delete into Record
- [ ] improve error contexts reported to callers (e.g. with `snafu` or improving use of `thiserror`)
- [ ] error handling and reporting in the event of a corrupt record
- [ ] investigate allowing the access of old values
- [x] file_id to FileId(u32)
- [x] key_size to KeySize(u16)
- [x] value_size to ValueSize(u32)
- [ ] tx_id to `time::Time`?
- [x] use crc32 instead of blake3

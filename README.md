# b2

An implementation of [Bitcask](https://riak.com/assets/bitcask-intro.pdf) as a Rust library.

## when would something like this make sense?

From a user's perspective, you can think of B2 (and Bitcask) as a disk-backed hashmap.
Operationally, there is more to it than that, but "hashmap, but on disk" is a good first approximation of how you use it.

This kind of storage model might make sense for you if:
- a simple key/value information model is enough for your domain
- pure in-memory storage does not work for you; values must be stored on disk
- you require type flexibility; values can be any types that implement `Serialize` and `DeserializeOwned`
- read and write latency are important to your domain
- your keyspace (all keys your database knows about) fits in system memory

## what can it do

This is the public API:

```rust
pub async fn new(db_directory: &Path, options: Options) -> Result<Self>
pub async fn get<V: Serialize + DeserializeOwned + Send>(&self, key: &K) -> Result<Option<V>>
pub async fn insert<V: Serialize + DeserializeOwned + Send>(&self, k: K, v: V) -> Result<()>
pub async fn remove(&self, k: K) -> Result<()>
pub async fn keys(&self) -> Vec<K>
pub async fn contains_key(&self, k: &K) -> bool
pub async fn merge(&self) -> Result<()>
pub async fn flush(&self) -> Result<()>
pub fn db_directory(&self) -> &Path
```

For a given database, keys must all be the same type (i.e., all `String`, or whatever other type can implement `Serialize` and `DeserializeOwned`). This may be relaxed at some point.

Values can vary arbitrarily, again as long as they can be serialized and deserialized. This means that for values, B2 is effectively dynamically typed/late bound. Values on disk are just bytes, and they are given a type when you insert/get them.

In terms of concurrency, right now B2 uses a coarse-grained `tokio::sync::RwLock`, so there can be: `(N readers) XOR (1 writer)`. Given Bitcask's model, it should be possible to relax this so that there can be `(N readers) AND (1 writer)`, and I might do that in the future.

By default B2 flushes every write to disk. This is slow, but leads to predictable read-after-write semantics. You can relax this (and increase write throughput at the expense of read-after-write serializability) by changing an option.

See the Bitcask paper to understand in more detail why Bitcask's particular conception of a key/value store is unique and interesting and why it might or might not make sense for your requirements.

## is it any good? should I use it?

Right now, probably not! From what I can tell, B2 is API complete with respect to the Bitcask paper. This does not mean it functions correctly. It is undertested. It uses a simple `tokio::sync::RwLock` internally so its concurrency story is weaker than it could be. There are probably other problems with it. Nonetheless, it is a tiny amount of code in comparison to other database systems (<1500 lines), so you can probably actually understand what this does just by reading the source.

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
- [ ] investigate allowing the access of old values rather than having the keydir refer to only the most recent value
- [x] investigate relaxing `K` to callsite-level rather than database-level (decision: not right now. this would require either making `K` be `Box<dyn KeydirKey>` or serializing `K` and having every access of the keydir require a serialization, at minimum)
- [x] file_id to FileId(u32)
- [x] key_size to KeySize(u16)
- [x] value_size to ValueSize(u32)
- [ ] tx_id to `time::Time`?
- [x] use crc32 instead of blake3
- [x] make internal write buffer size configurable

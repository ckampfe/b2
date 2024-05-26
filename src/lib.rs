//! todo:
//! workout the locking scheme for:
//! - "regular read" operations (get, etc)
//! - "regular write" operations (insert, remove)
//! - "special write" operations (flush, merge)
//!
//! my thinking right now is that regular read and regular write operations
//! can happen concurrently. they should not block each other at all
//!
//! special write operations should be exclusive.
//! they block both regular read *and* regular write operations.

use crate::base::Base;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

mod base;
pub mod error;
mod keydir;
mod loadable;
mod merge_pointer;
mod record;

pub type Result<T> = std::result::Result<T, error::Error>;

#[derive(Clone, Debug)]
pub struct Options {
    // 256 MiB by default
    pub max_file_size_bytes: u64,
    /// defaults to u32::max >> 3,
    /// as per https://docs.rs/tokio/latest/tokio/sync/struct.RwLock.html#method.with_max_readers
    pub max_readers: u32,
    pub flush_behavior: FlushBehavior,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_file_size_bytes: 2u64.pow(28),
            max_readers: 536870911,
            flush_behavior: FlushBehavior::default(),
        }
    }
}

/// Governs the when the internal in-memory write buffer is flushed to disk.
/// If the process is killed before the buffer is flushed,
/// its contents are lost.
///
/// `AfterEveryWrite` means the buffer is flushed after
/// every single `insert` or `remove`, regardless.
/// This is more durable, but gives up write throughput.
///
/// `WhenFull` means the buffer is flushed to disk when full.
/// This is faster, but gives up some durability.
///
#[derive(Clone, Debug, Default, PartialEq)]
pub enum FlushBehavior {
    /// flush the internal write buffer to disk on every single `insert` and `remove`
    #[default]
    AfterEveryWrite,
    /// only flush the internal write buffer to disk when it is full.
    /// this has no time guarantees whatsoever, so
    /// call `flush` manually if read-after-write is imporant to you
    /// when using this option
    WhenFull,
}

#[derive(Clone, Debug)]
pub struct B2<K>
where
    K: Eq + Hash + Serialize + DeserializeOwned + Send,
{
    db_directory: PathBuf,
    base: Arc<RwLock<Base<K>>>,
}

impl<K> B2<K>
where
    K: Eq + Hash + Serialize + DeserializeOwned + Send,
{
    /// Opens the database in the given directory, creating it if it does not exist.
    pub async fn open(db_directory: &Path, options: Options) -> Result<Self> {
        assert!(options.max_file_size_bytes > 0);

        let base = Arc::new(RwLock::with_max_readers(
            Base::new(db_directory, options.clone()).await?,
            options.max_readers,
        ));

        Ok(Self {
            db_directory: db_directory.to_owned(),
            base,
        })
    }

    pub async fn get<V: Serialize + DeserializeOwned + Send>(&self, key: &K) -> Result<Option<V>> {
        let base = self.base.read().await;
        base.get(key).await
    }

    pub async fn insert<V: Serialize + DeserializeOwned + Send>(&self, k: K, v: V) -> Result<()> {
        let mut base = self.base.write().await;
        base.insert(k, v).await
    }

    pub async fn remove(&self, k: K) -> Result<()> {
        let mut base = self.base.write().await;
        base.remove(k).await
    }

    pub async fn contains_key(&self, k: &K) -> bool {
        let base = self.base.read().await;
        base.contains_key(k)
    }

    pub async fn merge(&self) -> Result<()> {
        let mut base = self.base.write().await;
        base.merge().await
    }

    /// provided to allow a manual flush of the internal write buffer.
    /// this is normally governed by the `FlushBehavior` setting on `Options`.
    pub async fn flush(&self) -> Result<()> {
        let mut base = self.base.write().await;
        base.flush().await
    }
}

impl<K> B2<K>
where
    K: Clone + Eq + Hash + DeserializeOwned + Serialize + Send,
{
    pub async fn keys(&self) -> Vec<K> {
        let base = self.base.read().await;
        base.keys().cloned().collect()
    }
}

// impl<
//         K: Eq + Hash + Serialize + DeserializeOwned + Send,
//         V: Serialize + DeserializeOwned + Send,
//     > Drop for B2<K, V>
// {
//     fn drop(&mut self) {
//         std::thread::scope(|s| {
//             s.spawn(|| {
//                 let rt = tokio::runtime::Builder::new_current_thread()
//                     .build()
//                     .unwrap();
//                 rt.block_on(async {
//                     // TODO is sync_all/sync_data necessary here?
//                     let mut base = self.base.write().await;
//                     let _ = base.flush().await.unwrap();
//                 });
//             });
//         });
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn simple_roundtrip() {
        let dir = temp_dir::TempDir::with_prefix("b2").unwrap();

        let db = B2::open(dir.path(), Options::default()).await.unwrap();

        let k = "foo".to_string();
        let v = "bar".to_string();

        db.insert(k.clone(), v.clone()).await.unwrap();

        let challenge: String = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge, v);
    }

    #[tokio::test]
    async fn simple_write_then_delete() {
        let dir = temp_dir::TempDir::with_prefix("b2").unwrap();

        let db = B2::open(dir.path(), Options::default()).await.unwrap();

        let k = "foo".to_string();
        let v = "bar".to_string();

        db.insert(k.clone(), v.clone()).await.unwrap();

        let challenge: String = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge, v);

        db.remove(k.clone()).await.unwrap();

        let challenge2: Option<String> = db.get(&k).await.unwrap();

        assert_eq!(challenge2, None);
    }

    #[tokio::test]
    async fn simple_write_then_drop_then_load() {
        let dir = temp_dir::TempDir::with_prefix("b2").unwrap();

        let db = B2::open(dir.path(), Options::default()).await.unwrap();

        let k = "foo".to_string();
        let v = "bar".to_string();

        db.insert(k.clone(), v.clone()).await.unwrap();

        let challenge: String = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge, v);

        drop(db);

        let db: B2<String> = B2::open(dir.path(), Options::default()).await.unwrap();

        let challenge: String = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge, v);
    }

    #[tokio::test]
    async fn delete_then_write() {
        let dir = temp_dir::TempDir::with_prefix("b2").unwrap();

        let db: B2<String> = B2::open(dir.path(), Options::default()).await.unwrap();

        let k = "foo".to_string();
        let v = "bar".to_string();

        assert!(!db.contains_key(&k).await);

        // note that k has not been inserted yet
        db.remove(k.clone()).await.unwrap();

        assert!(!db.contains_key(&k).await);

        let challenge1: Option<String> = db.get(&k).await.unwrap();

        assert_eq!(challenge1, None);

        db.insert(k.clone(), v.clone()).await.unwrap();

        let challenge2: String = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge2, v);

        drop(db);

        let db: B2<String> = B2::open(dir.path(), Options::default()).await.unwrap();

        let challenge3: String = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge3, v)
    }

    #[tokio::test]
    async fn varied_value_types_in_the_same_db() {
        let dir = temp_dir::TempDir::with_prefix("b2").unwrap();

        let db: B2<String> = B2::open(dir.path(), Options::default()).await.unwrap();

        let k1 = "foo".to_string();
        let k2 = "bar".to_string();
        let k3 = "baz".to_string();

        // three different types of values in the same db
        let v1: String = "bar".to_string();
        let v2: u32 = 99;
        let v3: [i128; 3] = [-1, -2, -3];

        db.insert(k1.clone(), v1.clone()).await.unwrap();
        db.insert(k2.clone(), v2.clone()).await.unwrap();
        db.insert(k3.clone(), v3.clone()).await.unwrap();

        let c1: String = db.get(&k1).await.unwrap().unwrap();
        assert_eq!(c1, v1);

        let c2: u32 = db.get(&k2).await.unwrap().unwrap();
        assert_eq!(c2, v2);

        let c3: [i128; 3] = db.get(&k3).await.unwrap().unwrap();
        assert_eq!(c3, v3);
    }

    #[tokio::test]
    async fn merge_simple() {
        let dir = temp_dir::TempDir::with_prefix("b2").unwrap();

        let db: B2<String> = B2::open(dir.path(), Options::default()).await.unwrap();

        let k = "some key".to_string();

        let v1 = "v1".to_string();
        let v2 = "v2".to_string();
        let v3 = "v3".to_string();

        db.insert(k.clone(), v1.clone()).await.unwrap();
        assert_eq!(db.get::<String>(&k).await.unwrap().unwrap(), v1);
        db.insert(k.clone(), v2.clone()).await.unwrap();
        assert_eq!(db.get::<String>(&k).await.unwrap().unwrap(), v2);
        db.insert(k.clone(), v3.clone()).await.unwrap();
        assert_eq!(db.get::<String>(&k).await.unwrap().unwrap(), v3);

        assert_eq!(get_files(&dir.path()).await.len(), 1);

        drop(db);

        let db: B2<String> = B2::open(dir.path(), Options::default()).await.unwrap();

        assert_eq!(get_files(&dir.path()).await.len(), 2);

        assert_eq!(db.get::<String>(&k).await.unwrap().unwrap(), v3);

        db.merge().await.unwrap();

        assert_eq!(get_files(&dir.path()).await.len(), 2);

        assert_eq!(db.get::<String>(&k).await.unwrap().unwrap(), v3);
    }

    #[tokio::test]
    async fn merge_delete() {
        let dir = temp_dir::TempDir::with_prefix("b2").unwrap();

        let db: B2<String> = B2::open(dir.path(), Options::default()).await.unwrap();

        let k = "some key".to_string();

        let v1 = "v1".to_string();
        let v2 = "v2".to_string();
        let v3 = "v3".to_string();

        db.insert(k.clone(), v1.clone()).await.unwrap();
        assert_eq!(db.get::<String>(&k).await.unwrap().unwrap(), v1);
        db.insert(k.clone(), v2.clone()).await.unwrap();
        assert_eq!(db.get::<String>(&k).await.unwrap().unwrap(), v2);
        db.insert(k.clone(), v3.clone()).await.unwrap();
        assert_eq!(db.get::<String>(&k).await.unwrap().unwrap(), v3);

        db.remove(k.clone()).await.unwrap();

        drop(db);

        let db: B2<String> = B2::open(dir.path(), Options::default()).await.unwrap();

        db.merge().await.unwrap();

        assert_eq!(db.contains_key(&k).await, false);

        assert_eq!(get_files(&dir.path()).await.len(), 1);
    }

    #[tokio::test]
    async fn merge_same_key() {
        let dir = temp_dir::TempDir::with_prefix("b2").unwrap();

        let db: B2<String> = B2::open(dir.path(), Options::default()).await.unwrap();

        let k = "some key".to_string();

        let v1 = "v1".to_string();
        let v2 = "v2".to_string();

        db.insert(k.clone(), v1.clone()).await.unwrap();

        drop(db);

        let db: B2<String> = B2::open(dir.path(), Options::default()).await.unwrap();

        db.insert(k.clone(), v2.clone()).await.unwrap();

        db.merge().await.unwrap();

        assert_eq!(db.get::<String>(&k).await.unwrap().unwrap(), v2);

        assert_eq!(get_files(&dir.path()).await.len(), 1);
    }

    async fn get_files<P: AsRef<Path>>(dir: &P) -> Vec<PathBuf> {
        let mut s = tokio::fs::read_dir(dir).await.unwrap();

        let mut entries = vec![];

        while let Some(e) = s.next_entry().await.unwrap() {
            if e.path().is_file() {
                entries.push(e.path());
            }
        }

        entries
    }
}

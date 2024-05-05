use crate::base::Base;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

mod base;
pub mod error;
mod keydir;

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
pub struct B2<K, V>
where
    K: Eq + Hash + Serialize + DeserializeOwned + Send,
    V: Serialize + DeserializeOwned + Send,
{
    db_directory: PathBuf,
    base: Arc<RwLock<Base<K, V>>>,
}

impl<K, V> B2<K, V>
where
    K: Eq + Hash + Serialize + DeserializeOwned + Send,
    V: Serialize + DeserializeOwned + Send,
{
    pub async fn new(db_directory: &Path, options: Options) -> Result<Self> {
        let base = Arc::new(RwLock::with_max_readers(
            Base::new(db_directory, options.clone()).await?,
            options.max_readers,
        ));

        Ok(Self {
            db_directory: db_directory.to_owned(),
            base,
        })
    }

    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        let base = self.base.read().await;
        base.get(key).await
    }

    pub async fn insert(&self, k: K, v: V) -> Result<()> {
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
        todo!()
    }

    /// provided to allow a manual flush of the internal write buffer.
    /// this is normally governed by the `FlushBehavior` setting on `Options`.
    pub async fn flush(&self) -> Result<()> {
        let mut base = self.base.write().await;
        base.flush().await
    }
}

impl<K, V> B2<K, V>
where
    K: Clone + Eq + Hash + DeserializeOwned + Serialize + Send,
    V: Serialize + DeserializeOwned + Send,
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

        let db = B2::new(dir.path(), Options::default()).await.unwrap();

        let k = "foo".to_string();
        let v = "bar".to_string();

        db.insert(k.clone(), v.clone()).await.unwrap();

        let challenge = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge, v);
    }

    #[tokio::test]
    async fn simple_write_then_delete() {
        let dir = temp_dir::TempDir::with_prefix("b2").unwrap();

        let db = B2::new(dir.path(), Options::default()).await.unwrap();

        let k = "foo".to_string();
        let v = "bar".to_string();

        db.insert(k.clone(), v.clone()).await.unwrap();

        let challenge = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge, v);

        db.remove(k.clone()).await.unwrap();

        let challenge2 = db.get(&k).await.unwrap();

        assert_eq!(challenge2, None);
    }

    #[tokio::test]
    async fn simple_write_then_drop_then_load() {
        let dir = temp_dir::TempDir::with_prefix("b2").unwrap();

        let db = B2::new(dir.path(), Options::default()).await.unwrap();

        let k = "foo".to_string();
        let v = "bar".to_string();

        db.insert(k.clone(), v.clone()).await.unwrap();

        let challenge = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge, v);

        drop(db);

        let db: B2<String, String> = B2::new(dir.path(), Options::default()).await.unwrap();

        let challenge: String = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge, v);
    }

    #[tokio::test]
    async fn delete_then_write() {
        let dir = temp_dir::TempDir::with_prefix("b2").unwrap();

        let db: B2<String, String> = B2::new(dir.path(), Options::default()).await.unwrap();

        let k = "foo".to_string();
        let v = "bar".to_string();

        assert!(!db.contains_key(&k).await);

        // note that k has not been inserted yet
        db.remove(k.clone()).await.unwrap();

        assert!(!db.contains_key(&k).await);

        let challenge1 = db.get(&k).await.unwrap();

        assert_eq!(challenge1, None);

        db.insert(k.clone(), v.clone()).await.unwrap();

        let challenge2 = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge2, v);

        drop(db);

        let db: B2<String, String> = B2::new(dir.path(), Options::default()).await.unwrap();

        let challenge3 = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge3, v)
    }
}

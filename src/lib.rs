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
    pub max_file_size_bytes: u64,
    /// defaults to u32::max >> 3,
    /// as per https://docs.rs/tokio/latest/tokio/sync/struct.RwLock.html#method.with_max_readers
    pub max_readers: u32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_file_size_bytes: 2u64.pow(28),
            max_readers: 536870911,
        }
    }
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

    pub async fn insert(&self, k: K, v: V) -> Result<Option<V>> {
        let mut base = self.base.write().await;
        base.insert(k, v).await
    }

    pub async fn remove(&self, k: K) -> Result<Option<V>> {
        let mut base = self.base.write().await;
        base.remove(k).await
    }

    pub async fn merge(&self) -> Result<()> {
        todo!()
    }

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

        db.flush().await.unwrap();

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

        db.flush().await.unwrap();

        let challenge = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge, v);

        db.remove(k.clone()).await.unwrap();

        db.flush().await.unwrap();

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

        db.flush().await.unwrap();

        let challenge = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge, v);

        drop(db);

        let db: B2<String, String> = B2::new(dir.path(), Options::default()).await.unwrap();

        let challenge: String = db.get(&k).await.unwrap().unwrap();

        assert_eq!(challenge, v);
    }
}

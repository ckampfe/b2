use crate::keydir::{EntryPointer, EntryWithLiveness, FileId, Keydir, Liveness};
use crate::loadable::Loadable;
use crate::merge_pointer::MergePointer;
use crate::record::{Record, TxId};
use crate::Options;
use crate::{error, FlushBehavior};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

#[derive(Debug)]
pub(crate) struct Base<K>
where
    K: Eq + Hash + Serialize + DeserializeOwned + Send,
{
    db_directory: PathBuf,
    options: Options,
    keydir: Keydir<K>,
    active_file: tokio::io::BufWriter<tokio::fs::File>,
    active_file_id: FileId,
    offset: u64,
    tx_id: TxId,
}

// public impls
impl<K> Base<K>
where
    K: Eq + Hash + Serialize + DeserializeOwned + Send,
{
    pub(crate) async fn new(db_directory: &Path, options: Options) -> crate::Result<Self> {
        let mut db_file_ids = Self::all_db_file_ids(db_directory).await?;

        db_file_ids.sort();

        let file_id_zero: FileId = 0.into();

        let latest_file_id = db_file_ids.last().unwrap_or(&file_id_zero);

        let active_file_id = latest_file_id + 1;

        let all_entries_with_livenesses: HashMap<K, EntryWithLiveness> =
            <EntryWithLiveness as Loadable<K>>::load_latest_entries(db_directory, &db_file_ids)
                .await?;

        let all_entries = all_entries_with_livenesses
            .into_iter()
            .filter_map(|(key, entry_with_liveness)| {
                if entry_with_liveness.liveness == Liveness::Deleted {
                    None
                } else {
                    Some((key, entry_with_liveness.entry))
                }
            })
            .collect();

        let keydir = Keydir::new(all_entries);

        let latest_tx_id = keydir.latest_tx_id().unwrap_or(0.into());

        let mut active_file_path = db_directory.to_owned();
        active_file_path.push(active_file_id.to_string());

        let active_file = tokio::fs::File::options()
            .append(true)
            .create_new(true)
            .open(active_file_path)
            .await?;

        let active_file = tokio::io::BufWriter::new(active_file);

        Ok(Self {
            db_directory: db_directory.to_owned(),
            options,
            keydir,
            active_file,
            active_file_id,
            offset: 0,
            tx_id: latest_tx_id + 1,
        })
    }

    pub(crate) async fn get<V: Serialize + DeserializeOwned + Send>(
        &self,
        k: &K,
    ) -> crate::Result<Option<V>> {
        if let Some(entry) = self.keydir.get(k) {
            let mut path = self.db_directory.clone();
            path.push(entry.file_id.to_string());

            let mut f = tokio::fs::File::open(path).await?;

            f.seek(std::io::SeekFrom::Start(entry.value_position))
                .await?;

            let mut buf = vec![0u8; entry.value_size.0 as usize];

            f.read_exact(&mut buf).await?;

            let v: V = bincode::deserialize(&buf).map_err(|e| error::DeserializeError {
                msg: "unable to deserialize from bincode".to_string(),
                source: e,
            })?;

            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn insert<V: Serialize + DeserializeOwned + Send>(
        &mut self,
        k: K,
        v: V,
    ) -> crate::Result<()> {
        self.write_insert(k, v).await
    }

    pub(crate) async fn remove(&mut self, k: K) -> crate::Result<()> {
        if self.keydir.contains_key(&k) {
            self.write_delete(k).await
        } else {
            Ok(())
        }
    }

    pub(crate) fn contains_key(&self, k: &K) -> bool {
        self.keydir.contains_key(k)
    }

    pub(crate) fn keys(&self) -> std::collections::hash_map::Keys<'_, K, EntryPointer> {
        self.keydir.keys()
    }

    /// # invariants
    ///
    /// ### affected data files
    /// - only inactive data files may be merged.
    /// - the active data file SHALL NOT be changed at all.
    /// - opening a new database ALWAYS opens a new active file, no matter what
    ///
    /// ### latest data
    /// - for a given key `K`, only the record with the latest TXID
    ///   may remain in the dataset.
    /// - if the record for a given key `K` with the latest TXID is a delete,
    ///   it SHALL NOT appear in the inactive data files
    ///
    /// ### cannot add data
    /// - the total size of all data on disk AFTER merge
    ///   SHALL be less than or equal to the total size of all data
    ///   on disk BEFORE merge.
    ///
    /// ### no dangling files
    /// - all database files SHALL have a size > 0 bytes.
    pub(crate) async fn merge(&mut self) -> crate::Result<()> {
        // TODO
        // take current keydir into account when determining whether to keep
        // a given merge record.
        // when writing a merge record, only write it if it does not appear in the
        // active file
        let mut inactive_db_files = self.inactive_db_file_ids().await?;

        let merge_pointers = <MergePointer as Loadable<K>>::load_latest_entries(
            &self.db_directory,
            &inactive_db_files,
        )
        .await?;

        let live_merge_pointers = merge_pointers
            .into_iter()
            .filter(|(_key, merge_pointer)| merge_pointer.liveness == Liveness::Live);

        // let mut current_write_file = tokio::io::BufWriter::new(write_file);
        let mut current_write_file: Option<tokio::io::BufWriter<tokio::fs::File>> = None;

        let mut offset = 0;

        for (key, merge_pointer) in live_merge_pointers {
            //
            if let Some(entry) = self.keydir.get(&key) {
                // IFF the entry has been written to the active file,
                // do not process it at all, as its latest version
                // is later than anything seen by the merge process,
                // as the merge process only touches inactive files
                if entry.tx_id > merge_pointer.tx_id {
                    continue;
                }
            }

            let mut current_write_file_id = inactive_db_files.pop().unwrap();

            let mut current_file_path = self.db_directory.to_owned();
            let mut current_file_name = current_write_file_id.to_string();
            current_file_name.push_str(".merge");
            current_file_path.push(current_file_name);

            // db directory
            // merge pointers
            // max file size in bytes
            // nonactive_file_ids
            if let Some(write_file) = current_write_file.as_mut() {
                if offset > self.options.max_file_size_bytes {
                    write_file.flush().await?;

                    current_write_file_id = inactive_db_files.pop().unwrap();
                    // current_file_path = self.db_directory.to_owned();
                    self.db_directory.clone_into(&mut current_file_path);
                    current_file_name = current_write_file_id.to_string();
                    current_file_name.push_str(".merge");
                    current_file_path.push(current_file_name);

                    let write_file = tokio::fs::File::options()
                        .append(true)
                        .create_new(true)
                        .open(&current_file_path)
                        .await?;

                    current_write_file = Some(tokio::io::BufWriter::new(write_file));
                }
            } else {
                let write_file = tokio::fs::File::options()
                    .append(true)
                    .create_new(true)
                    .open(&current_file_path)
                    .await?;

                current_write_file = Some(tokio::io::BufWriter::new(write_file));
            }

            let mut reader_path = self.db_directory.to_owned();

            reader_path.push(merge_pointer.file_id.to_string());

            let read_file = tokio::fs::File::open(reader_path).await?;

            let mut read_file = tokio::io::BufReader::new(read_file);

            read_file
                .seek(std::io::SeekFrom::Start(merge_pointer.record_offset))
                .await?;

            let mut take_handle = read_file.take(merge_pointer.record_size);

            let bytes_read = {
                // DUMB, BAD
                let mut writer = current_write_file.unwrap();

                let bytes_read = tokio::io::copy_buf(&mut take_handle, &mut writer).await?;

                // DUMB, BAD
                current_write_file = Some(writer);

                bytes_read
            };

            assert!(bytes_read == merge_pointer.record_size);

            let value_position = offset
                + crate::record::Record::HEADER_SIZE as u64
                + merge_pointer.key_size.0 as u64;

            offset += merge_pointer.record_size;

            let new_entry = EntryPointer {
                file_id: current_write_file_id,
                value_position,
                value_size: merge_pointer.value_size,
                tx_id: merge_pointer.tx_id,
            };

            self.keydir.insert(key, new_entry);
        }

        if let Some(mut write_file) = current_write_file {
            write_file.flush().await?;
        }

        // rm all inactive db files
        for file_id in self.inactive_db_file_ids().await? {
            let mut filename = self.db_directory.clone();
            filename.push(file_id.to_string());
            tokio::fs::remove_file(filename).await?;
        }

        // rename all .merge files
        for merge_path in self.merge_files().await? {
            if tokio::fs::metadata(&merge_path).await?.len() > 0 {
                let normal_filename = merge_path.file_stem().unwrap();
                let mut normal_path = self.db_directory.clone();
                normal_path.push(normal_filename);
                tokio::fs::rename(&merge_path, normal_path).await?;
            } else {
                tokio::fs::remove_file(merge_path).await?;
            }
        }

        Ok(())
    }

    pub(crate) async fn flush(&mut self) -> crate::Result<()> {
        self.active_file.flush().await.map_err(|e| e.into())
    }
}

// private impls
impl<K> Base<K>
where
    K: Eq + Hash + Serialize + DeserializeOwned + Send,
{
    // TODO investigate whether we can collapse write_delete and write_insert
    async fn write_insert<V: Serialize + DeserializeOwned + Send>(
        &mut self,
        k: K,
        v: V,
    ) -> crate::Result<()> {
        self.tx_id += 1;

        let record = Record::new(&k, &v, self.tx_id)?;

        self.active_file.write_all(&record).await?;

        let value_position =
            self.offset + crate::record::Record::HEADER_SIZE as u64 + record.key_size().0 as u64;

        let entry = EntryPointer {
            file_id: self.active_file_id,
            value_position,
            value_size: record.value_size(),
            tx_id: self.tx_id,
        };

        self.keydir.insert(k, entry);

        let entry_size = crate::record::Record::HEADER_SIZE
            + record.key_size().0 as usize
            + record.value_size().0 as usize;

        self.offset += entry_size as u64;

        if self.offset >= self.options.max_file_size_bytes {
            self.active_file.flush().await?;

            self.active_file_id += 1;

            let mut new_active_file_path = self.db_directory.clone();

            new_active_file_path.push(self.active_file_id.to_string());

            let active_file = tokio::fs::File::options()
                .append(true)
                .create_new(true)
                .open(new_active_file_path)
                .await?;

            let active_file = tokio::io::BufWriter::new(active_file);
            self.active_file = active_file;
        }

        if self.options.flush_behavior == FlushBehavior::AfterEveryWrite {
            self.flush().await
        } else {
            Ok(())
        }
    }

    async fn write_delete(&mut self, k: K) -> crate::Result<()> {
        self.tx_id += 1;

        let v = Record::tombstone();

        let record = Record::new(&k, &v, self.tx_id)?;

        self.active_file.write_all(&record).await?;

        self.keydir.remove(&k);

        let entry_size = crate::record::Record::HEADER_SIZE
            + record.key_size().0 as usize
            + record.value_size().0 as usize;

        self.offset += entry_size as u64;

        if self.offset >= self.options.max_file_size_bytes {
            self.active_file.flush().await?;

            self.active_file_id += 1;

            let mut new_active_file_path = self.db_directory.clone();

            new_active_file_path.push(self.active_file_id.to_string());

            let active_file = tokio::fs::File::options()
                .append(true)
                .create_new(true)
                .open(new_active_file_path)
                .await?;

            let active_file = tokio::io::BufWriter::new(active_file);
            self.active_file = active_file;
        }

        if self.options.flush_behavior == FlushBehavior::AfterEveryWrite {
            self.flush().await
        } else {
            Ok(())
        }
    }

    async fn all_db_file_ids(db_directory: &Path) -> crate::Result<Vec<FileId>> {
        let mut file_ids = vec![];

        let mut dir_reader = tokio::fs::read_dir(db_directory).await?;

        while let Some(dir_entry) = dir_reader.next_entry().await? {
            if dir_entry.file_type().await?.is_file() {
                let path = dir_entry.path();
                let file_name = path.file_name().unwrap().to_owned();
                let file_name = file_name.to_str().unwrap();
                let file_name = file_name.to_owned();
                if let Ok(file_id) = file_name.parse() {
                    file_ids.push(file_id);
                }
            }
        }

        Ok(file_ids)
    }

    async fn inactive_db_file_ids(&self) -> crate::Result<Vec<FileId>> {
        let mut db_file_ids = Self::all_db_file_ids(&self.db_directory).await?;

        db_file_ids.retain(|file_id| *file_id != self.active_file_id);

        Ok(db_file_ids)
    }

    async fn merge_files(&self) -> crate::Result<Vec<PathBuf>> {
        let mut file_ids = vec![];

        let mut dir_reader = tokio::fs::read_dir(&self.db_directory).await?;

        while let Some(dir_entry) = dir_reader.next_entry().await? {
            if dir_entry.file_type().await?.is_file() {
                let path = dir_entry.path();

                if let Some(extension) = path.extension() {
                    if extension == "merge" {
                        file_ids.push(path);
                    }
                }
            }
        }

        Ok(file_ids)
    }
}

impl<K: Eq + Hash + Serialize + DeserializeOwned + Send> Drop for Base<K> {
    fn drop(&mut self) {
        std::thread::scope(|s| {
            s.spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    // TODO is sync_all/sync_data necessary here?
                    let _ = self.flush().await;
                });
            });
        });
    }
}

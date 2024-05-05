use crate::error;
use crate::keydir::{Entry, EntryWithLiveness, Keydir, Liveness};
use crate::Options;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

const HASH_SIZE: usize = blake3::OUT_LEN;
const TX_ID_SIZE: usize = std::mem::size_of::<u128>();
const KEY_SIZE_SIZE: usize = std::mem::size_of::<u32>();
const VALUE_SIZE_SIZE: usize = std::mem::size_of::<u32>();
const HEADER_SIZE: usize = HASH_SIZE + TX_ID_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE;

// const TOMBSTONE: [u8; 1] = [0];

#[derive(Serialize, Deserialize)]
struct Tombstone;

static TOMBSTONE: OnceLock<Vec<u8>> = OnceLock::new();

enum ValueOrDelete<V> {
    Value(V),
    Delete,
}

#[derive(Debug)]
pub(crate) struct Base<K, V>
where
    K: Eq + Hash + Serialize + DeserializeOwned + Send,
    V: Serialize + DeserializeOwned + Send,
{
    db_directory: PathBuf,
    options: Options,
    keydir: Keydir<K>,
    active_file: tokio::io::BufWriter<tokio::fs::File>,
    active_file_id: u64,
    offset: u64,
    tx_id: u128,
    _v: PhantomData<V>,
}

impl<K, V> Base<K, V>
where
    K: Eq + Hash + Serialize + DeserializeOwned + Send,
    V: Serialize + DeserializeOwned + Send,
{
    pub(crate) async fn new(db_directory: &Path, options: Options) -> crate::Result<Self> {
        let mut db_files = vec![];

        let mut dir_reader = tokio::fs::read_dir(db_directory).await?;

        while let Some(dir_entry) = dir_reader.next_entry().await? {
            if dir_entry.file_type().await?.is_file() {
                let path = dir_entry.path();
                let file_name = path.file_name().unwrap().to_owned();
                let file_name = file_name.to_str().unwrap();
                let file_name = file_name.to_owned();

                db_files.push(file_name);
            }
        }

        let mut db_file_ids: Vec<u64> = db_files
            .iter()
            .filter_map(|db_file| db_file.parse::<u64>().ok())
            .collect();

        db_file_ids.sort();

        let latest_file_id = db_file_ids.last().map(|id| id + 1).unwrap_or(0);

        let active_file_id = latest_file_id + 1;

        let mut all_files_entries = vec![];

        // TODO make parallel
        for file_id in db_file_ids {
            let file_entries = Self::load_file(db_directory, file_id).await?;
            all_files_entries.push(file_entries);
        }

        let mut all_entries_with_livenesses: HashMap<K, EntryWithLiveness> = HashMap::new();

        for file_entries in all_files_entries {
            for (key, potential_new_entry) in file_entries {
                if let Some(existing_entry) = all_entries_with_livenesses.get(&key) {
                    if potential_new_entry.entry.tx_id > existing_entry.entry.tx_id {
                        all_entries_with_livenesses.insert(key, potential_new_entry);
                    }
                } else {
                    all_entries_with_livenesses.insert(key, potential_new_entry);
                }
            }
        }

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

        let latest_tx_id = keydir.latest_tx_id().unwrap_or(0);

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
            _v: PhantomData,
        })
    }

    pub(crate) async fn get(&self, k: &K) -> crate::Result<Option<V>> {
        if let Some(entry) = self.keydir.get(k) {
            let mut path = self.db_directory.clone();
            path.push(entry.file_id.to_string());

            let mut f = tokio::fs::File::open(path).await?;

            f.seek(std::io::SeekFrom::Start(entry.value_position))
                .await?;

            let mut buf = vec![0u8; entry.value_size as usize];

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

    pub(crate) async fn insert(&mut self, k: K, v: V) -> crate::Result<Option<V>> {
        self.write(k, ValueOrDelete::Value(v)).await
    }

    pub(crate) async fn remove(&mut self, k: K) -> crate::Result<Option<V>> {
        self.write(k, ValueOrDelete::Delete).await
    }

    pub(crate) fn keys(&self) -> std::collections::hash_map::Keys<'_, K, Entry> {
        self.keydir.keys()
    }

    async fn write(&mut self, k: K, v: ValueOrDelete<V>) -> crate::Result<Option<V>> {
        // tx_id = tx_id + 1
        self.tx_id += 1;
        // encoded_tx_id = Binary.encode_u128_be(tx_id)
        let encoded_tx_id = self.tx_id.to_be_bytes();
        // encoded_key = Binary.encode_term(key)
        let encoded_key = bincode::serialize(&k).map_err(|e| error::SerializeError {
            msg: "unable to serialize to bincode".to_string(),
            source: e,
        })?;

        // encoded_value =
        //   case value do
        //     {:insert, v} ->
        //       Binary.encode_term(v)

        //     # pass through untouched,
        //     # as when reading we just do a direct binary comparison
        //     # against this value
        //     :delete ->
        //       Binary.tombstone()
        //   end
        let encoded_value = match v {
            ValueOrDelete::Value(ref v) => {
                bincode::serialize(v).map_err(|e| error::SerializeError {
                    msg: "unable to serialize to bincode".to_string(),
                    source: e,
                })?
            }
            ValueOrDelete::Delete => {
                bincode::serialize(&Tombstone).map_err(|e| error::SerializeError {
                    msg: "unable to serialize to bincode".to_string(),
                    source: e,
                })?
            }
        };

        // key_size =
        //   byte_size(encoded_key)
        let key_size = encoded_key.len();

        // value_size =
        //   byte_size(encoded_value)
        let value_size = encoded_value.len();

        // encoded_key_size = Binary.encode_u32_be(key_size)
        // encoded_value_size = Binary.encode_u32_be(value_size)
        let encoded_key_size = (key_size as u32).to_be_bytes();
        let encoded_value_size = (value_size as u32).to_be_bytes();

        // payload =
        //   [
        //     encoded_tx_id,
        //     encoded_key_size,
        //     encoded_value_size,
        //     encoded_key,
        //     encoded_value
        //   ]

        let mut payload = vec![];
        payload.extend_from_slice(&encoded_tx_id);
        payload.extend_from_slice(&encoded_key_size);
        payload.extend_from_slice(&encoded_value_size);
        payload.extend_from_slice(&encoded_key);
        payload.extend_from_slice(&encoded_value);

        // hash =
        //   Binary.hash(payload)
        let hash = blake3::hash(&payload);
        let hash = hash.as_bytes();

        // @hash_size = byte_size(hash)

        // # an iolist, so there is no further serialization,
        // # we write this directly to disk
        // entry = [hash, payload]

        // :ok = IO.binwrite(active_file, entry)
        // :ok = :file.datasync(active_file)
        self.active_file.write_all(hash).await?;
        self.active_file.write_all(&payload).await?;

        // value_position = offset + Binary.header_size() + key_size
        let value_position = self.offset + HEADER_SIZE as u64 + key_size as u64;

        // Keydir.insert(
        //   keydir,
        //   {
        //     key,
        //     active_file_id,
        //     value_size,
        //     value_position,
        //     tx_id
        //   }
        // )
        let entry = Entry {
            file_id: self.active_file_id,
            value_size: value_size.try_into().unwrap(),
            value_position,
            tx_id: self.tx_id,
        };

        match v {
            ValueOrDelete::Value(_) => {
                self.keydir.insert(k, entry);
            }
            ValueOrDelete::Delete => self.keydir.remove(k),
        }

        // entry_size = Binary.header_size() + key_size + value_size
        let entry_size = HEADER_SIZE + key_size + value_size;

        self.offset += entry_size as u64;

        // state =
        //   state
        //   |> Map.update!(:offset, fn offset ->
        //     offset + entry_size
        //   end)
        //   |> Map.put(:tx_id, tx_id)

        // state =
        //   if state[:offset] >= state[:options][:max_file_size_bytes] do
        //     File.close(active_file)

        //     active_file_id = active_file_id + 1

        //     {:ok, new_active_file} =
        //       [db_directory, to_string(active_file_id)]
        //       |> Path.join()
        //       |> File.open([:append, :raw])

        //     state
        //     |> Map.put(:active_file_id, active_file_id)
        //     |> Map.put(:offset, 0)
        //     |> Map.put(:active_file, new_active_file)
        //   else
        //     state
        //   end

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

        Ok(None)
    }

    async fn load_file(
        db_directory: &Path,
        file_id: u64,
    ) -> crate::Result<HashMap<K, EntryWithLiveness>> {
        let mut path = db_directory.to_owned();

        path.push(file_id.to_string());

        let f = tokio::fs::File::open(path).await?;

        let mut reader = tokio::io::BufReader::new(f);

        let mut entries = HashMap::new();

        let mut offset = 0;

        while let Some((k, entry_with_liveness)) =
            Self::read_records(&mut reader, &mut offset, file_id).await?
        {
            entries.insert(k, entry_with_liveness);
        }

        Ok(entries)
    }

    async fn read_records<R: AsyncRead + Unpin>(
        reader: &mut R,
        offset: &mut u64,
        file_id: u64,
    ) -> crate::Result<Option<(K, EntryWithLiveness)>> {
        let mut header = vec![0u8; HEADER_SIZE];

        // if we can't read any header, bytes it means we're done
        if let Err(e) = reader.read_exact(&mut header).await {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
        };

        // start header

        // TODO compare hashes
        let mut position = 0;
        let hash = &header[position..position + HASH_SIZE];
        position += HASH_SIZE;
        let encoded_tx_id = &header[position..position + TX_ID_SIZE];
        position += TX_ID_SIZE;
        let encoded_key_size = &header[position..position + KEY_SIZE_SIZE];
        position += KEY_SIZE_SIZE;
        let encoded_value_size = &header[position..position + VALUE_SIZE_SIZE];

        let tx_id = u128::from_be_bytes(encoded_tx_id.try_into().unwrap());
        let key_size = u32::from_be_bytes(encoded_key_size.try_into().unwrap());
        let value_size = u32::from_be_bytes(encoded_value_size.try_into().unwrap());
        // end header

        // start body
        let key_size_usize: usize = key_size.try_into().unwrap();
        let value_size_usize: usize = value_size.try_into().unwrap();
        let mut body = vec![0u8; key_size_usize + value_size_usize];
        // if we've already read header bytes and can't read body bytes,
        // it's an error
        reader.read_exact(&mut body).await?;

        position = 0;
        let encoded_key = &body[position..position + key_size_usize];
        position += key_size_usize;
        let encoded_value = &body[position..position + value_size_usize];
        // end body

        let value_position = *offset + HEADER_SIZE as u64 + key_size as u64;
        *offset += HEADER_SIZE as u64 + body.len() as u64;

        let liveness =
            if encoded_value == TOMBSTONE.get_or_init(|| bincode::serialize(&Tombstone).unwrap()) {
                Liveness::Deleted
            } else {
                Liveness::Live
            };

        let key = bincode::deserialize(encoded_key).map_err(|e| error::DeserializeError {
            msg: "unable to deserialize from bincode".to_string(),
            source: e,
        })?;

        Ok(Some((
            key,
            EntryWithLiveness {
                liveness,
                entry: Entry {
                    file_id,
                    value_size,
                    value_position,
                    tx_id,
                },
            },
        )))
    }

    pub(crate) async fn flush(&mut self) -> crate::Result<()> {
        self.active_file.flush().await.map_err(|e| e.into())
    }
}

impl<
        K: Eq + Hash + Serialize + DeserializeOwned + Send,
        V: Serialize + DeserializeOwned + Send,
    > Drop for Base<K, V>
{
    fn drop(&mut self) {
        std::thread::scope(|s| {
            s.spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                rt.block_on(async {
                    // TODO is sync_all/sync_data necessary here?
                    let _ = self.active_file.flush().await;
                });
            });
        });
    }
}

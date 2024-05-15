use crate::base::{Tombstone, TOMBSTONE};
use crate::loadable::Loadable;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::hash::Hash;
use tokio::io::AsyncRead;

#[derive(Debug)]
pub(crate) struct Keydir<K>
where
    K: Eq + Hash,
{
    keydir: HashMap<K, EntryPointer>,
}

impl<K> Keydir<K>
where
    K: Eq + Hash,
{
    pub(crate) fn new(hm: HashMap<K, EntryPointer>) -> Self {
        Keydir { keydir: hm }
    }

    pub(crate) fn insert(&mut self, k: K, entry: EntryPointer) -> Option<EntryPointer> {
        self.keydir.insert(k, entry)
    }

    pub(crate) fn get(&self, k: &K) -> Option<&EntryPointer> {
        self.keydir.get(k)
    }

    pub(crate) fn remove(&mut self, k: &K) -> Option<EntryPointer> {
        self.keydir.remove(k)
    }

    pub(crate) fn contains_key(&self, k: &K) -> bool {
        self.keydir.contains_key(k)
    }

    pub(crate) fn keys(&self) -> std::collections::hash_map::Keys<'_, K, EntryPointer> {
        self.keydir.keys()
    }

    pub(crate) fn latest_tx_id(&self) -> Option<u128> {
        self.keydir
            .values()
            .max_by(|a, b| a.tx_id.cmp(&b.tx_id))
            .map(|entry| entry.tx_id)
    }
}

#[derive(Debug)]
pub(crate) struct EntryPointer {
    pub(crate) file_id: u64,
    pub(crate) value_position: u64,
    pub(crate) value_size: u32,
    pub(crate) tx_id: u128,
}

#[derive(Debug)]
pub(crate) struct EntryWithLiveness {
    pub(crate) liveness: Liveness,
    pub(crate) entry: EntryPointer,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Liveness {
    Live,
    Deleted,
}

impl<K> Loadable<K> for EntryWithLiveness
where
    K: Eq + Hash + DeserializeOwned,
{
    async fn read<R: AsyncRead + Unpin>(
        reader: &mut tokio::io::BufReader<R>,
        offset: &mut u64,
        file_id: u64,
    ) -> crate::Result<Option<(K, Self)>>
    where
        Self: Sized,
    {
        // end header
        let record = match crate::record::Record::read_from(reader).await {
            Ok(option) => match option {
                Some(record) => record,
                None => {
                    return Ok(None);
                }
            },
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                } else {
                    return Err(e.into());
                }
            }
        };

        if !record.is_valid() {
            return Err(crate::error::Error::CorruptRecord);
        }

        let value_position =
            *offset + crate::record::Record::HEADER_SIZE as u64 + record.key_size() as u64;

        // and update the offset to reflect that we have read a record
        *offset += record.len() as u64;

        let liveness = if record.value_bytes()
            == TOMBSTONE.get_or_init(|| bincode::serialize(&Tombstone).unwrap())
        {
            Liveness::Deleted
        } else {
            Liveness::Live
        };

        let key = bincode::deserialize(record.key_bytes()).map_err(|e| {
            crate::error::DeserializeError {
                msg: "unable to deserialize from bincode".to_string(),
                source: e,
            }
        })?;

        Ok(Some((
            key,
            EntryWithLiveness {
                liveness,
                entry: EntryPointer {
                    file_id,
                    value_size: record.value_size(),
                    value_position,
                    tx_id: record.tx_id(),
                },
            },
        )))
    }

    fn tx_id(&self) -> u128 {
        self.entry.tx_id
    }
}

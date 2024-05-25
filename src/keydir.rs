use crate::loadable::Loadable;
use crate::record::{TxId, ValueSize};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::hash::Hash;
use std::num::ParseIntError;
use std::ops::{Add, AddAssign, Deref};
use std::str::FromStr;
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

    pub(crate) fn latest_tx_id(&self) -> Option<TxId> {
        self.keydir
            .values()
            .max_by(|a, b| a.tx_id.cmp(&b.tx_id))
            .map(|entry| entry.tx_id)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct EntryPointer {
    pub(crate) file_id: FileId,
    pub(crate) value_position: u64,
    pub(crate) value_size: ValueSize,
    pub(crate) tx_id: TxId,
}

#[derive(Debug, PartialEq)]
pub(crate) struct EntryWithLiveness {
    pub(crate) liveness: Liveness,
    pub(crate) entry: EntryPointer,
}

impl PartialOrd for EntryWithLiveness {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.entry.tx_id.partial_cmp(&other.entry.tx_id)
    }
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
        file_id: FileId,
    ) -> crate::Result<Option<(K, Self)>>
    where
        Self: Sized,
    {
        // end header
        let record = match crate::record::Record::read_from(reader).await {
            Ok(record) => record,
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

        let key = record.key()?;

        let liveness = record.liveness();

        let value_position =
            *offset + crate::record::Record::HEADER_SIZE as u64 + record.key_size().0 as u64;

        // and update the offset to reflect that we have read a record
        *offset += record.len() as u64;

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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct FileId(u32);

impl FromStr for FileId {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let v = s.parse::<u32>()?;
        Ok(FileId(v))
    }
}

impl From<u32> for FileId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl AddAssign<u32> for FileId {
    fn add_assign(&mut self, rhs: u32) {
        self.0 += rhs
    }
}

impl Add<u32> for &FileId {
    type Output = FileId;

    fn add(self, rhs: u32) -> Self::Output {
        FileId(self.0 + rhs)
    }
}

impl Deref for FileId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

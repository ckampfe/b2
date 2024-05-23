use crate::keydir::Liveness;
use crate::loadable::Loadable;
use serde::de::DeserializeOwned;
use std::hash::Hash;
use tokio::io::AsyncRead;

/// points to data in a db file,
/// used for merging db files
#[derive(PartialEq)]
pub(crate) struct MergePointer {
    /// whether the data is an insert or a delete
    pub(crate) liveness: Liveness,
    pub(crate) file_id: u64,
    pub(crate) tx_id: u128,
    pub(crate) record_offset: u64,
    pub(crate) record_size: u64,
    pub(crate) key_size: u32,
    pub(crate) value_size: u32,
}

impl PartialOrd for MergePointer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.tx_id.partial_cmp(&other.tx_id)
    }
}

impl<K: Eq + Hash + DeserializeOwned> Loadable<K> for MergePointer {
    async fn read<R: AsyncRead + Unpin>(
        reader: &mut tokio::io::BufReader<R>,
        offset: &mut u64,
        file_id: u64,
    ) -> crate::Result<Option<(K, Self)>> {
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

        let out = MergePointer {
            liveness,
            file_id,
            tx_id: record.tx_id(),
            record_offset: *offset,
            record_size: record.len() as u64,
            key_size: record.key_size(),
            value_size: record.value_size(),
        };

        *offset += record.len() as u64;

        Ok(Some((key, out)))
    }
}

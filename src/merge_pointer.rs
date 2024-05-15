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
    /// the absolute position of the beginning of the body section of this record in the file
    pub(crate) body_position: u64,
    /// the length of the body
    pub(crate) body_size: u64,
    // raw_record: Vec<u8>,
    // todo this should be something smaller, I think it's
    // a fixed-length thing that is just HEADER_LENGTH.
    // a u8?
    // hash_size + tx_id_size + key_size_size + value_size_size
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

        let body_position = *offset + crate::record::Record::HEADER_SIZE as u64;

        *offset += record.len() as u64;

        Ok(Some((
            key,
            MergePointer {
                liveness,
                file_id,
                tx_id: record.tx_id(),
                body_position,
                body_size: record.body_len().try_into().unwrap(),
                key_size: record.key_size(),
                value_size: record.value_size(),
            },
        )))
    }
}

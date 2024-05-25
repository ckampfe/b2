use crate::{error, keydir::Liveness};
use serde::{de::DeserializeOwned, Serialize};
use std::{ops::Deref, sync::OnceLock};
use tokio::io::{AsyncRead, AsyncReadExt};

const TOMBSTONE_BYTES: &[u8] = b"bitcask_tombstone";

static SERIALIZED_TOMBSTONE: OnceLock<Vec<u8>> = OnceLock::new();

/// A record is a "header" and a "body"
/// The header is (in on-disk and in-memory order):
/// - hash (the paper calls this `crc`) (4 bytes)
/// - tx_id (the paper calls this `tstamp`) (16 bytes)
/// - key_size (4 bytes)
/// - value_size (4 bytes)
///
/// The body is (also in on-disk and in-memory order):
/// - key
/// - value
pub(crate) struct Record {
    buf: Vec<u8>,
}

impl Deref for Record {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

// crate-public impls
impl Record {
    pub(crate) const HEADER_SIZE: usize =
        Record::HASH_SIZE + Record::TX_ID_SIZE + Record::KEY_SIZE_SIZE + Record::VALUE_SIZE_SIZE;

    pub(crate) fn new<K: Serialize, V: Serialize>(
        k: &K,
        v: &V,
        tx_id: u128,
    ) -> crate::Result<Self> {
        let encoded_tx_id = tx_id.to_be_bytes();

        let encoded_key = bincode::serialize(k).map_err(|e| error::SerializeError {
            msg: "unable to serialize to bincode".to_string(),
            source: e,
        })?;

        let encoded_value = bincode::serialize(v).map_err(|e| error::SerializeError {
            msg: "unable to serialize to bincode".to_string(),
            source: e,
        })?;

        let key_size = encoded_key.len();
        let value_size = encoded_value.len();
        let body_size = key_size + value_size;

        let encoded_key_size = (key_size as u32).to_be_bytes();
        let encoded_value_size = (value_size as u32).to_be_bytes();

        let mut buf = Vec::with_capacity(Self::HEADER_SIZE + body_size);
        // header
        // dummy hash bytes, added back in at the end...
        buf.extend_from_slice(&[0u8; Self::HASH_SIZE]);
        // rest of header
        buf.extend_from_slice(&encoded_tx_id);
        buf.extend_from_slice(&encoded_key_size);
        buf.extend_from_slice(&encoded_value_size);
        // body
        buf.extend_from_slice(&encoded_key);
        buf.extend_from_slice(&encoded_value);

        let hash = crc32fast::hash(&buf[Self::HASH_SIZE..]);
        let hash_bytes = hash.to_be_bytes();
        // ...and finally set the first 32 bytes to the hash
        buf[..Self::HASH_SIZE].copy_from_slice(&hash_bytes);

        Ok(Record { buf })
    }

    pub(crate) async fn read_from<R: AsyncRead + Unpin>(
        reader: &mut tokio::io::BufReader<R>,
    ) -> std::io::Result<Record> {
        let buf = vec![0u8; Record::HEADER_SIZE];

        let mut record = Record { buf };

        reader.read_exact(&mut record.buf).await?;

        let key_size_usize: usize = record.key_size().try_into().unwrap();
        let value_size_usize: usize = record.value_size().try_into().unwrap();
        let body_size: usize = key_size_usize + value_size_usize;

        record.buf.resize(record.buf.len() + body_size, 0);

        let body = &mut record.buf[Record::HEADER_SIZE..];

        reader.read_exact(body).await?;

        Ok(record)
    }

    pub(crate) fn key<K: DeserializeOwned>(&self) -> Result<K, crate::error::DeserializeError> {
        bincode::deserialize(self.key_bytes()).map_err(|e| crate::error::DeserializeError {
            msg: "unable to deserialize from bincode".to_string(),
            source: e,
        })
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.hash_read_from_disk() == self.computed_hash()
    }

    pub(crate) fn liveness(&self) -> Liveness {
        if self.value_bytes()
            == SERIALIZED_TOMBSTONE.get_or_init(|| bincode::serialize(&TOMBSTONE_BYTES).unwrap())
        {
            Liveness::Deleted
        } else {
            Liveness::Live
        }
    }

    pub(crate) fn tombstone() -> &'static [u8] {
        TOMBSTONE_BYTES
    }

    pub(crate) fn key_bytes(&self) -> &[u8] {
        let start = 0;
        let end = self.key_size() as usize;
        &self.body()[start..end]
    }

    pub(crate) fn value_bytes(&self) -> &[u8] {
        let start = self.key_size() as usize;
        let end = start + self.value_size() as usize;
        &self.body()[start..end]
    }

    pub(crate) fn len(&self) -> usize {
        self.buf.len()
    }

    pub(crate) fn tx_id(&self) -> u128 {
        u128::from_be_bytes(self.tx_id_bytes().try_into().unwrap())
    }

    pub(crate) fn key_size(&self) -> u32 {
        u32::from_be_bytes(self.key_size_bytes().try_into().unwrap())
    }

    pub(crate) fn value_size(&self) -> u32 {
        u32::from_be_bytes(self.value_size_bytes().try_into().unwrap())
    }
}

// private impls
impl Record {
    const HASH_SIZE: usize = std::mem::size_of::<u32>();
    const TX_ID_SIZE: usize = std::mem::size_of::<u128>();
    const KEY_SIZE_SIZE: usize = std::mem::size_of::<u32>();
    const VALUE_SIZE_SIZE: usize = std::mem::size_of::<u32>();

    fn header(&self) -> &[u8] {
        &self.buf[..Self::HEADER_SIZE]
    }

    fn body(&self) -> &[u8] {
        &self.buf[Self::HEADER_SIZE..]
    }

    fn hash_read_from_disk(&self) -> u32 {
        let hash_bytes = &self.header()[..Self::HASH_SIZE];
        u32::from_be_bytes(hash_bytes.try_into().unwrap())
    }

    fn computed_hash(&self) -> u32 {
        let mut hasher = crc32fast::Hasher::new();

        hasher.update(self.tx_id_bytes());
        hasher.update(self.key_size_bytes());
        hasher.update(self.value_size_bytes());
        hasher.update(self.body());

        hasher.finalize()
    }

    fn tx_id_bytes(&self) -> &[u8] {
        let start = Self::HASH_SIZE;
        let end = start + Self::TX_ID_SIZE;
        &self.header()[start..end]
    }

    fn key_size_bytes(&self) -> &[u8] {
        let start = Self::HASH_SIZE + Self::TX_ID_SIZE;
        let end = start + Self::KEY_SIZE_SIZE;
        &self.header()[start..end]
    }

    fn value_size_bytes(&self) -> &[u8] {
        let start = Self::HASH_SIZE + Self::TX_ID_SIZE + Self::KEY_SIZE_SIZE;
        let end = start + Self::VALUE_SIZE_SIZE;
        &self.header()[start..end]
    }
}

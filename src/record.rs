use tokio::io::{AsyncRead, AsyncReadExt};

pub(crate) struct Record {
    buf: Vec<u8>,
}

impl Record {
    pub(crate) const HEADER_SIZE: usize =
        Record::HASH_SIZE + Record::TX_ID_SIZE + Record::KEY_SIZE_SIZE + Record::VALUE_SIZE_SIZE;

    pub(crate) async fn read_from<R: AsyncRead + Unpin>(
        reader: &mut tokio::io::BufReader<R>,
    ) -> std::io::Result<Option<Record>> {
        let buf = vec![0u8; Record::HEADER_SIZE];

        let mut record = Record { buf };

        reader.read_exact(&mut record.buf).await?;

        let key_size_usize: usize = record.key_size().try_into().unwrap();
        let value_size_usize: usize = record.value_size().try_into().unwrap();
        let body_size: usize = key_size_usize + value_size_usize;

        record.buf.resize(record.buf.len() + body_size, 0);

        let body = &mut record.buf[Record::HEADER_SIZE..];

        reader.read_exact(body).await?;

        Ok(Some(record))
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.hash_read_from_disk() == self.computed_hash()
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

    pub(crate) fn body_len(&self) -> usize {
        self.body().len()
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

impl Record {
    const HASH_SIZE: usize = blake3::OUT_LEN;
    const TX_ID_SIZE: usize = std::mem::size_of::<u128>();
    const KEY_SIZE_SIZE: usize = std::mem::size_of::<u32>();
    const VALUE_SIZE_SIZE: usize = std::mem::size_of::<u32>();

    fn header(&self) -> &[u8] {
        &self.buf[..Self::HEADER_SIZE]
    }

    fn body(&self) -> &[u8] {
        &self.buf[Self::HEADER_SIZE..]
    }

    fn hash_read_from_disk(&self) -> blake3::Hash {
        let hash = &self.header()[0..Record::HASH_SIZE];
        blake3::Hash::from_bytes(hash.try_into().unwrap())
    }

    fn computed_hash(&self) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new();

        hasher.update(self.tx_id_bytes());
        hasher.update(self.key_size_bytes());
        hasher.update(self.value_size_bytes());
        hasher.update(self.body());

        hasher.finalize()
    }

    fn tx_id_bytes(&self) -> &[u8] {
        let start = Record::HASH_SIZE;
        let end = start + Record::TX_ID_SIZE;
        &self.header()[start..end]
    }

    fn key_size_bytes(&self) -> &[u8] {
        let start = Record::HASH_SIZE + Record::TX_ID_SIZE;
        let end = start + Record::KEY_SIZE_SIZE;
        &self.header()[start..end]
    }

    fn value_size_bytes(&self) -> &[u8] {
        let start = Record::HASH_SIZE + Record::TX_ID_SIZE + Record::KEY_SIZE_SIZE;
        let end = start + Record::VALUE_SIZE_SIZE;
        &self.header()[start..end]
    }
}

use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug)]
pub(crate) struct Keydir<K>
where
    K: Eq + Hash,
{
    keydir: HashMap<K, Entry>,
}

impl<K> Keydir<K>
where
    K: Eq + Hash,
{
    pub(crate) fn new(hm: HashMap<K, Entry>) -> Self {
        Keydir { keydir: hm }
    }

    pub(crate) fn insert(&mut self, k: K, entry: Entry) -> Option<Entry> {
        self.keydir.insert(k, entry)
    }

    pub(crate) fn get(&self, k: &K) -> Option<&Entry> {
        self.keydir.get(k)
    }

    pub(crate) fn remove(&mut self, k: &K) -> Option<Entry> {
        self.keydir.remove(&k)
    }

    pub(crate) fn contains_key(&self, k: &K) -> bool {
        self.keydir.contains_key(k)
    }

    pub(crate) fn keys(&self) -> std::collections::hash_map::Keys<'_, K, Entry> {
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
pub(crate) struct Entry {
    pub(crate) file_id: u64,
    pub(crate) value_size: u32,
    pub(crate) value_position: u64,
    pub(crate) tx_id: u128,
}

pub(crate) struct EntryWithLiveness {
    pub(crate) liveness: Liveness,
    pub(crate) entry: Entry,
}

#[derive(PartialEq)]
pub(crate) enum Liveness {
    Live,
    Deleted,
}

// struct FileId;
// struct ValueSize(u32);
// struct ValuePosition(u64);
// struct TxId(u128);

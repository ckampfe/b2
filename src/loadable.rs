use crate::keydir::FileId;
use std::hash::Hash;
use std::{collections::HashMap, path::Path};
use tokio::io::AsyncRead;

pub(crate) trait Loadable<K: Eq + Hash>: PartialOrd + Sized {
    async fn load_latest_entries(
        db_directory: &Path,
        db_file_ids: &[FileId],
    ) -> crate::Result<HashMap<K, Self>> {
        let mut all_files_entries: Vec<HashMap<K, Self>> = vec![];

        for file_id in db_file_ids {
            let file_entries = Self::load_entries_from_file(db_directory, *file_id).await?;
            all_files_entries.push(file_entries);
        }

        let mut all_entries_with_livenesses: HashMap<K, Self> = HashMap::new();

        for file_entries in all_files_entries {
            for (key, potential_new_entry) in file_entries {
                if let Some(existing_entry) = all_entries_with_livenesses.get(&key) {
                    if &potential_new_entry > existing_entry {
                        all_entries_with_livenesses.insert(key, potential_new_entry);
                    }
                } else {
                    all_entries_with_livenesses.insert(key, potential_new_entry);
                }
            }
        }

        Ok(all_entries_with_livenesses)
    }

    async fn load_entries_from_file(
        db_directory: &Path,
        file_id: FileId,
    ) -> crate::Result<HashMap<K, Self>> {
        let mut path = db_directory.to_owned();

        path.push(file_id.to_string());

        let f = tokio::fs::File::open(path).await?;

        let mut reader = tokio::io::BufReader::new(f);

        let mut entries = HashMap::new();

        let mut offset = 0;

        while let Some((k, entry_with_liveness)) =
            Self::read(&mut reader, &mut offset, file_id).await?
        {
            entries.insert(k, entry_with_liveness);
        }

        Ok(entries)
    }

    async fn read<R: AsyncRead + Unpin>(
        reader: &mut tokio::io::BufReader<R>,
        offset: &mut u64,
        file_id: FileId,
    ) -> crate::Result<Option<(K, Self)>>
    where
        Self: Sized;
}

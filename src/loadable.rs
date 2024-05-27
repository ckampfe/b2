use crate::keydir::FileId;
use std::hash::Hash;
use std::{collections::HashMap, path::Path};
use tokio::io::AsyncRead;

/// a trait that expresses that a type knows how to read
/// exactly one instance of itself from a file
pub(crate) trait Loadable<K: Eq + Hash>: PartialOrd + Sized {
    async fn read_one<R: AsyncRead + Unpin>(
        reader: &mut tokio::io::BufReader<R>,
        offset: &mut u64,
        file_id: FileId,
    ) -> crate::Result<Option<(K, Self)>>
    where
        Self: Sized;
}

pub(crate) async fn load_latest_entries<K, L>(
    db_directory: &Path,
    db_file_ids: &[FileId],
) -> crate::Result<HashMap<K, L>>
where
    K: Eq + Hash,
    L: Loadable<K>,
{
    let mut all_files_entries: Vec<HashMap<K, L>> = vec![];

    // TODO parallelize this
    for file_id in db_file_ids {
        let file_entries = load_all_entries_from_file(db_directory, *file_id).await?;
        all_files_entries.push(file_entries);
    }

    let mut all_entries: HashMap<K, L> = HashMap::new();

    for file_entries in all_files_entries {
        for (key, new_entry) in file_entries {
            if let Some(existing_entry) = all_entries.get(&key) {
                if &new_entry > existing_entry {
                    all_entries.insert(key, new_entry);
                }
            } else {
                all_entries.insert(key, new_entry);
            }
        }
    }

    Ok(all_entries)
}

async fn load_all_entries_from_file<K, L>(
    db_directory: &Path,
    file_id: FileId,
) -> crate::Result<HashMap<K, L>>
where
    K: Eq + Hash,
    L: Loadable<K>,
{
    let mut path = db_directory.to_owned();

    path.push(file_id.to_string());

    let f = tokio::fs::File::open(path).await?;

    let mut reader = tokio::io::BufReader::new(f);

    let mut entries = HashMap::new();

    let mut offset = 0;

    while let Some((k, entry_with_liveness)) =
        L::read_one(&mut reader, &mut offset, file_id).await?
    {
        entries.insert(k, entry_with_liveness);
    }

    Ok(entries)
}

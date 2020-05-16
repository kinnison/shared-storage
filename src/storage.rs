//! See the crate level documentation for more

use fehler::{throw, throws};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use std::collections::HashMap;
use std::convert::TryFrom;
use std::ffi::{OsStr, OsString};
use std::ops::Deref;
use std::path::{Path, PathBuf};

use crate::entry::*;
use crate::Error;
use crate::{ResourceAllocation, ResourceProvider};

struct InMemoryIndex {
    dir: Directory,
    dirty: bool,
}

impl From<Directory> for InMemoryIndex {
    fn from(dir: Directory) -> Self {
        Self { dir, dirty: false }
    }
}

pub struct SharedStorage {
    base: PathBuf,
    indices: HashMap<OsString, InMemoryIndex>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StorageIdentifier {
    hash: String,
    size: usize,
    executable: bool,
}

const DATA: &str = "data";
const INDICES: &str = "indices";
const MAX_METADATA_SIZE: u64 = 1 * 1024 * 1024;

impl SharedStorage {
    #[throws(Error)]
    pub async fn new<P: AsRef<Path>>(base: P) -> Self {
        let mut ret = Self {
            base: base.as_ref().to_owned(),
            indices: HashMap::new(),
        };
        ret.prepare_paths().await?;
        ret.load_indices().await?;
        ret
    }

    #[throws(Error)]
    async fn load_indices(&mut self) {
        let mut indexfiles = fs::read_dir(self.base.join(INDICES))
            .await
            .map_err(Error::Preparing)?;
        let mut try_remove = Vec::new();
        while let Some(entry) = indexfiles.next_entry().await.map_err(Error::Preparing)? {
            let meta = entry.metadata().await.map_err(Error::Preparing)?;
            if meta.is_file() {
                if meta.len() > MAX_METADATA_SIZE {
                    throw!(Error::IndexTooLarge(entry.path(), meta.len()))
                }
                let body = fs::read_to_string(&entry.path())
                    .await
                    .map_err(Error::Preparing)?;
                let dir = Directory::try_from(body.as_ref()).map_err(Error::ParsingIndex)?;
                self.indices.insert(entry.file_name(), dir.into());
            } else {
                try_remove.push(entry.path());
            }
        }
    }

    #[throws(Error)]
    async fn prepare_paths(&self) {
        fs::create_dir_all(&self.base)
            .await
            .map_err(Error::Preparing)?;
        fs::create_dir_all(self.base.join(DATA))
            .await
            .map_err(Error::Preparing)?;
        fs::create_dir_all(self.base.join(INDICES))
            .await
            .map_err(Error::Preparing)?;
    }

    #[throws(Error)]
    async fn save_index(&mut self, name: &OsStr) {
        let ime = self.indices.get_mut(name).unwrap_or_else(|| {
            panic!("Internal error: Attempted to save unknown index {:?}", name)
        });
        if ime.dirty {
            let dir_s = String::try_from(&ime.dir).map_err(Error::SerialisingIndex)?;
            let s_len = u64::try_from(dir_s.len())
                .map_err(|_| Error::IndexTooLarge(name.into(), std::u64::MAX))?;
            if s_len > MAX_METADATA_SIZE {
                throw!(Error::IndexTooLarge(name.into(), s_len));
            }
            let index_path = self.base.join(INDICES).join(name);
            let index_path_tmp = index_path.join(".tmp");
            let mut fh = fs::OpenOptions::new()
                .read(false)
                .write(true)
                .create_new(true)
                .open(&index_path_tmp)
                .await
                .map_err(|e| Error::WritingIndex(index_path_tmp.to_owned(), e))?;
            fh.write_all(dir_s.as_bytes())
                .await
                .map_err(|e| Error::WritingIndex(index_path_tmp.to_owned(), e))?;
            // Complete any pending background IO
            fh.flush()
                .await
                .map_err(|e| Error::WritingIndex(index_path_tmp.to_owned(), e))?;
            // Having flushed we can drop the fh to know it's closed
            drop(fh);
            match fs::rename(&index_path_tmp, &index_path).await {
                Err(e) => {
                    fs::remove_file(&index_path_tmp).await.unwrap_or(());
                    throw!(Error::WritingIndex(index_path.to_owned(), e))
                }
                Ok(()) => {}
            }
            ime.dirty = false;
        }
    }

    // Public methods from here

    pub fn base(&self) -> &Path {
        &self.base
    }

    pub fn indices(&self) -> impl Iterator<Item = &OsStr> {
        self.indices.keys().map(Deref::deref)
    }

    //#[throws(Error)]
    //pub async fn import_tar<P, R, C, S>(
    //    &mut self,
    //    name: S,
    //    skip_first: bool,
    //    path: P,
    //    res: &mut dyn ResourceProvider<ResourceClaim = C>,
    //) where
    //    P: AsRef<Path>,
    //    S: AsRef<OsStr>,
    //    C: Send + Sync,
    //{
    //    let path = path.as_ref();
    //    let name = name.as_ref();
    //    let mut root = Directory::default();
    //
    //    let mut root: InMemoryIndex = root.into();
    //    root.dirty = true;
    //    self.indices.insert(name.to_owned(), root);
    //    match self.save_index(name).await {
    //        Ok(_) => {}
    //        Err(e) => {
    //            self.indices.remove(name);
    //            throw!(e);
    //        }
    //    }
    //}
}

#[cfg(test)]
mod test {
    use super::SharedStorage;

    #[tokio::test]
    async fn create_twice() {
        let td = tempfile::tempdir().expect("Unable to create tempdir");
        let ss = SharedStorage::new(&td)
            .await
            .expect("Unable to create storage");
        drop(ss);
        let ss = SharedStorage::new(&td)
            .await
            .expect("Unable to create storage a second time");
        drop(ss);
    }
}

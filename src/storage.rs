//! See the crate level documentation for more

use bytes::Bytes;
use fehler::{throw, throws};
use futures::future::{BoxFuture, FutureExt};
use futures::stream::FuturesUnordered;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::{self, AsyncWriteExt};

use std::collections::HashMap;
use std::convert::TryFrom;
use std::ffi::{OsStr, OsString};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::task::Poll;

use crate::entry::*;
use crate::Error;
use crate::{ResourceAllocation, ResourceClaimResult, ResourceProvider};

const DATA: &str = "data";
const INDICES: &str = "indices";
const MAX_METADATA_SIZE: u64 = 1 * 1024 * 1024;

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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageIdentifier {
    hash: String,
    size: usize,
    executable: bool,
}

/// Events yielded to the import process by whatever import stream
/// is generating them.
pub enum ImportEvent {
    /// A directory which needs to be created in the index
    Directory(PathBuf),
    /// A file which needs to be creted in the index.
    /// If the pathbuf is present, then it is the path inside which the file
    /// should be placed.  The filename is next, and then the size in bytes of
    /// the file.  Finally the boolean is true if the file needs to be marked
    /// as executable.  The file's data must not be loaded for this event.
    File(Option<PathBuf>, OsString, usize, bool),
    /// The data for the previous File event.  The file's data is not loaded into
    /// memory until this event is drawn from the stream.
    FileData(Bytes),
    /// An error of some kind has occurred in the stream and import should be
    /// aborted.
    Error(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl StorageIdentifier {
    fn filename(&self, base: &Path) -> PathBuf {
        // Our structure is done as XX/YY/.......
        // In theory that means the dirs contain at most 256 entries at the
        // upper levels, and then 1 or 2 at the end
        // the filename is RESTOFHASH-SIZEx
        // where the x is present if executable
        let mut prefix = base.to_owned();
        prefix.push(DATA);
        prefix.push(&self.hash[0..2]);
        prefix.push(&self.hash[2..4]);
        prefix.push(format!(
            "{}-{}{}",
            &self.hash[4..],
            self.size,
            if self.executable { "x" } else { "" }
        ));
        prefix
    }
}

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
            let index_path_tmp = {
                let mut ret = index_path.clone();
                ret.set_extension("tmp");
                ret
            };
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

    #[throws(Error)]
    pub async fn import<Claim, Name, Contents>(
        &mut self,
        name: Name,
        provider: &mut dyn ResourceProvider<ResourceClaim = Claim>,
        content: Contents,
    ) where
        Name: AsRef<OsStr>,
        Claim: ResourceAllocation + 'static,
        Contents: Stream<Item = ImportEvent> + Unpin,
    {
        let name = name.as_ref();
        let mut root = Directory::default();
        let mut inserters: FuturesUnordered<
            BoxFuture<Result<(Option<PathBuf>, OsString, StorageIdentifier), Error>>,
        > = FuturesUnordered::new();

        match self
            .import_(content, &mut root, &mut inserters, provider)
            .await
        {
            Err(e) => {
                while let Some(_) = inserters.next().await {}
                throw!(e);
            }
            Ok(_) => {
                while let Some((parent_path, file_name, identity)) =
                    inserters.next().await.transpose()?
                {
                    if let Some(parent_path) = parent_path {
                        root.traverse_mut(&parent_path, false)?
                            .insert_file(file_name, identity)?;
                    } else {
                        root.insert_file(file_name, identity)?;
                    }
                }
            }
        }
        assert!(inserters.is_empty());
        drop(inserters);

        let mut root: InMemoryIndex = root.into();
        root.dirty = true;
        self.indices.insert(name.to_owned(), root);
        match self.save_index(name).await {
            Ok(_) => {}
            Err(e) => {
                self.indices.remove(name);
                throw!(e);
            }
        }
    }

    #[throws(Error)]
    async fn import_<'a, Contents, Claim>(
        &'a mut self,
        mut content: Contents,
        root: &mut Directory,
        inserters: &mut FuturesUnordered<
            BoxFuture<'a, Result<(Option<PathBuf>, OsString, StorageIdentifier), Error>>,
        >,
        provider: &mut dyn ResourceProvider<ResourceClaim = Claim>,
    ) where
        Contents: Stream<Item = ImportEvent> + Unpin,
        Claim: ResourceAllocation + 'static,
    {
        let mut event_ = content.next().await;
        while let Some(event) = event_.take() {
            // Before we do anything else, try and deal with any inserters
            // who have completed
            loop {
                match futures::poll!(inserters.next()) {
                    Poll::Ready(Some(Err(e))) => throw!(e),
                    Poll::Ready(Some(Ok((parent_path, file_name, identity)))) => {
                        if let Some(parent_path) = parent_path {
                            root.traverse_mut(&parent_path, false)?
                                .insert_file(file_name, identity)?;
                        } else {
                            root.insert_file(file_name, identity)?;
                        }
                    }
                    _ => break,
                }
            }
            match event {
                ImportEvent::Error(e) => throw!(Error::ImportStreamError(e)),
                ImportEvent::FileData(_) => throw!(Error::UnexpectedFileData),
                ImportEvent::Directory(d) => {
                    if let Some(dirname) = d.file_name() {
                        if let Some(parent) = d.parent() {
                            root.traverse_mut(parent, false)?.mkdir(dirname)?;
                        } else {
                            root.mkdir(dirname)?;
                        };
                    }
                }
                ImportEvent::File(parent_path, file_name, size, executable) => {
                    // We're trying to insert this file, so first we need
                    // an allocation in order to make this possible
                    let mut alloc = loop {
                        let maybe_alloc = provider.claim(size).await;
                        match maybe_alloc {
                            ResourceClaimResult::Impossible => throw!(Error::ImpossibleFileClaim(
                                parent_path
                                    .as_deref()
                                    .unwrap_or(Path::new(""))
                                    .join(file_name),
                                size
                            )),
                            ResourceClaimResult::Busy => {
                                if let Some((parent_path, file_name, identity)) =
                                    inserters.next().await.transpose()?
                                {
                                    if let Some(parent_path) = parent_path {
                                        root.traverse_mut(&parent_path, false)?
                                            .insert_file(file_name, identity)?;
                                    } else {
                                        root.insert_file(file_name, identity)?;
                                    }
                                }
                            }
                            ResourceClaimResult::Ok(claim) => break claim,
                        }
                    };
                    // We have an allocation, let's draw the next event
                    // which must be file data
                    match content.next().await {
                        None => {
                            alloc.release().await;
                            throw!(Error::UnexpectedEndOfContent)
                        }
                        Some(ImportEvent::FileData(bytes)) => {
                            // We have an allocation, we have the bytes, let's
                            // spawn our future
                            inserters.push(
                                tokio::task::spawn(Self::import_file(
                                    alloc,
                                    self.base().to_owned(),
                                    parent_path,
                                    file_name,
                                    executable,
                                    bytes,
                                ))
                                .map(|r| r.unwrap_or_else(|e| Err(Error::JoinError(e))))
                                .boxed(),
                            );
                        }
                        Some(ImportEvent::Error(e)) => throw!(Error::ImportStreamError(e)),
                        _ => {
                            alloc.release().await;
                            throw!(Error::ExpectedFileDataEvent)
                        }
                    }
                }
            }
            event_ = content.next().await;
        }
    }

    #[throws(Error)]
    async fn import_file(
        mut allocation: impl ResourceAllocation,
        base_path: PathBuf,
        parent_path: Option<PathBuf>,
        file_name: OsString,
        executable: bool,
        contents: Bytes,
    ) -> (Option<PathBuf>, OsString, StorageIdentifier) {
        use bytes::Buf;
        // Rough approach is as follows...
        // First we compute the identifier for the input data and decide
        // if we already have it.
        let size = contents.len();
        let hash = tokio::task::block_in_place(|| {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.input(contents.bytes());
            let result = hasher.result();
            format!("{:x}", result)
        });
        let identity = StorageIdentifier {
            hash,
            size,
            executable,
        };
        // Next we need to see if we need to insert it into the store
        // to do that we determine the file name and then see if we can stat it
        let entry_path = identity.filename(&base_path);
        match fs::metadata(&entry_path).await {
            Ok(_) => {}
            Err(e) => {
                if e.kind() == io::ErrorKind::NotFound {
                    // The file wasn't present, so we need to prepare the
                    // prefix directories
                    let parent = entry_path.parent().unwrap();
                    fs::create_dir_all(parent)
                        .await
                        .map_err(|e| Error::IOErrorAddingToStorage(entry_path.clone(), e))?;
                    let mut temp_file = entry_path.clone();
                    temp_file.set_extension("tmp");
                    let mut fh = fs::OpenOptions::new()
                        .read(false)
                        .write(true)
                        .create_new(true)
                        .open(&temp_file)
                        .await
                        .map_err(|e| Error::IOErrorAddingToStorage(temp_file.clone(), e))?;
                    fh.write_all(contents.bytes())
                        .await
                        .map_err(|e| Error::IOErrorAddingToStorage(temp_file.clone(), e))?;
                    // Complete any pending background IO
                    fh.flush()
                        .await
                        .map_err(|e| Error::IOErrorAddingToStorage(temp_file.clone(), e))?;
                    // Having flushed we can drop the fh to know it's closed
                    drop(fh);
                    fs::rename(&temp_file, &entry_path)
                        .await
                        .map_err(|e| Error::IOErrorAddingToStorage(temp_file.clone(), e))?;
                } else {
                    throw!(Error::IOErrorAddingToStorage(entry_path, e));
                }
            }
        }

        // Clean up our memory usage
        drop(contents);
        allocation.release().await;
        (parent_path, file_name, identity)
    }
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

//! Useful utility stuff for shared storage
//!

use async_trait::async_trait;
use fehler::throws;
use futures::future::BoxFuture;
use futures::stream::unfold;
use futures::Stream;
use tokio::fs;
use tokio::sync::Mutex;

use std::convert::TryFrom;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::storage::ImportEvent;
use crate::{ResourceAllocation, ResourceClaimResult, ResourceProvider};

type AMSRPInner = Arc<Mutex<SRPInner>>;

/// A simple resource provider which offers claim count and memory limits
///
/// Since claims often imply file handles, it makes good sense to limit the
/// number of active claims during an import cycle.  The simple resource
/// provider will hard-limit based on claim count.  It also makes sense to
/// limit concurrent memory usage since importing into a storage requires that
/// the data for the file is held in memory until it has been hashed.  Given
/// that, the simple resource provider has a size limit.  However it's not
/// ideal to hard-limit that since single claims might exceed the space requirements
/// and so we soft-limit RAM so we can't exceed the space limit set unless
/// it's the only claim.
///
/// The SRP is written in an async mode rather then using threaded synchronisation
/// because it is used exclusively in async environments.
///
/// In brief, create one, and give it to the import process you want limited.
pub struct SimpleResourceProvider {
    inner: AMSRPInner,
}

#[derive(Default)]
struct SRPInner {
    claims: usize,
    claims_in_use: usize,
    space: usize,
    space_in_use: usize,
    max_space: Option<usize>,
}

impl SimpleResourceProvider {
    // Create a resource provider soft-limited on space
    pub fn new(claims: usize, space: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SRPInner {
                claims,
                space,
                ..SRPInner::default()
            })),
        }
    }

    // Create a resource provider with a soft-limit and a hard-limit on space
    // if max_space is smaller than space then individual claims will be limited
    // to less than the space available
    // Remember that claim attempts for more than max_space will return Impossible
    // which will terminate an import process more often than not.
    pub fn new_with_max_space(claims: usize, space: usize, max_space: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SRPInner {
                claims,
                space,
                max_space: Some(max_space),
                ..SRPInner::default()
            })),
        }
    }
}

pub struct SimpleResourceAllocation {
    inner: AMSRPInner,
    space: usize,
    released: bool,
}

#[async_trait]
impl ResourceAllocation for SimpleResourceAllocation {
    async fn release(&mut self) {
        if !self.released {
            let mut inner = self.inner.lock().await;
            inner.claims_in_use -= 1;
            inner.space_in_use -= self.space;
            self.released = true;
        }
    }

    fn released(&self) -> bool {
        self.released
    }
}

#[async_trait]
impl ResourceProvider for SimpleResourceProvider {
    type ResourceClaim = SimpleResourceAllocation;

    async fn claim(&self, size: usize) -> ResourceClaimResult<Self::ResourceClaim> {
        let mut inner = self.inner.lock().await;
        match inner.max_space {
            Some(max) if size > max => ResourceClaimResult::Impossible,
            _ => {
                if inner.claims_in_use == inner.claims {
                    // Busy because too many claims
                    ResourceClaimResult::Busy
                } else if inner.claims_in_use > 0 && (inner.space_in_use + size) > inner.space {
                    // Busy because too much space currently in use
                    ResourceClaimResult::Busy
                } else {
                    // Doesn't exceed claim count or space in use, so give back a claim
                    inner.space_in_use += size;
                    inner.claims_in_use += 1;
                    ResourceClaimResult::Ok(SimpleResourceAllocation {
                        inner: self.inner.clone(),
                        space: size,
                        released: false,
                    })
                }
            }
        }
    }

    async fn claims_in_use(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.claims_in_use
    }

    async fn spare_claims(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.claims - inner.claims_in_use
    }

    async fn space_in_use(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.space_in_use
    }

    async fn spare_space(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.space.saturating_sub(inner.space_in_use)
    }
}

drop_claim_impl!(SimpleResourceAllocation);

/// A filesystem import stream usable with SharedStorage::import
///
#[derive(Debug)]
pub struct FSImportStream {
    entries: Vec<FSEntry>,
    state: FSIMachine,
    base_path: PathBuf,
}

#[derive(Debug)]
enum FSIMachine {
    Start,
    Finished,
    Next(usize),
    Data(usize),
}

#[derive(Debug)]
enum FSEntry {
    Dir(PathBuf),
    File(Option<PathBuf>, OsString, usize, bool),
}

impl FSImportStream {
    #[throws(tokio::io::Error)]
    pub async fn new<P: AsRef<Path>>(base_path: P) -> Self {
        let mut entries: Vec<FSEntry> = Vec::new();
        let mut base_path = base_path.as_ref().to_owned();
        let mut sub_path = PathBuf::new();
        Self::scan_dir(&mut base_path, &mut sub_path, &mut entries).await?;
        Self {
            entries,
            state: FSIMachine::Start,
            base_path,
        }
    }

    fn scan_dir<'a>(
        fs_path: &'a mut PathBuf,
        sub_path: &'a mut PathBuf,
        entries: &'a mut Vec<FSEntry>,
    ) -> BoxFuture<'a, Result<(), tokio::io::Error>> {
        Box::pin(async move {
            let mut reader = fs::read_dir(&fs_path).await?;
            while let Some(entry) = reader.next_entry().await? {
                let meta = entry.metadata().await?;
                if meta.is_dir() {
                    fs_path.push(entry.file_name());
                    sub_path.push(entry.file_name());
                    entries.push(FSEntry::Dir(sub_path.clone()));
                    Self::scan_dir(fs_path, sub_path, entries).await?;
                    fs_path.pop();
                    sub_path.pop();
                } else if meta.is_file() {
                    let executable = is_executable(&meta);
                    let len: usize = usize::try_from(meta.len())
                        .expect("Cannot work with files bigger than virtual memory, sorry");
                    entries.push(FSEntry::File(
                        if sub_path.parent().is_some() {
                            Some(sub_path.clone())
                        } else {
                            None
                        },
                        entry.file_name().clone(),
                        len,
                        executable,
                    ))
                }
            }
            Ok(())
        })
    }

    async fn next_event(mut self) -> Option<(ImportEvent, Self)> {
        use FSIMachine::*;
        loop {
            break match std::mem::replace(&mut self.state, Finished) {
                Start => {
                    if self.entries.is_empty() {
                        None
                    } else {
                        self.state = Next(0);
                        continue;
                    }
                }
                Finished => None,
                Data(n) => {
                    if let FSEntry::File(pd, fname, _, _) = &self.entries[n] {
                        let full_path = self.base_path.join(if let Some(pd) = pd {
                            pd.join(fname)
                        } else {
                            fname.into()
                        });
                        let data = match fs::read(full_path).await {
                            Ok(data) => data,
                            Err(e) => return Some((ImportEvent::Error(e.into()), self)),
                        };
                        self.state = Next(n + 1);
                        Some((ImportEvent::FileData(data.into()), self))
                    } else {
                        None
                    }
                }
                Next(n) => {
                    if n == self.entries.len() {
                        None
                    } else {
                        match &self.entries[n] {
                            FSEntry::Dir(p) => {
                                self.state = Next(n + 1);
                                Some((ImportEvent::Directory(p.clone()), self))
                            }
                            FSEntry::File(pd, fname, size, exec) => {
                                self.state = Data(n);
                                Some((
                                    ImportEvent::File(pd.clone(), fname.clone(), *size, *exec),
                                    self,
                                ))
                            }
                        }
                    }
                }
            };
        }
    }

    pub fn into_stream(self) -> impl Stream<Item = ImportEvent> {
        Box::pin(unfold(self, Self::next_event))
    }
}

#[cfg(windows)]
fn is_executable(_meta: &std::fs::Metadata) -> bool {
    false
}
#[cfg(not(windows))]
fn is_executable(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    (meta.mode() & 0o111) != 0
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::stream::StreamExt;
    use tempfile::TempDir;

    #[tokio::test]
    async fn basic_claims() {
        let res = SimpleResourceProvider::new(5, 100);
        {
            let mut claims = Vec::new();
            for _ in 0..5 {
                claims.push(res.claim(10).await.unwrap());
            }
            assert_eq!(res.claims_in_use().await, 5);
            assert_eq!(res.space_in_use().await, 50);
            for mut claim in claims.drain(..) {
                claim.release().await;
            }
        }
        assert_eq!(res.claims_in_use().await, 0);
        assert_eq!(res.space_in_use().await, 0);
    }

    #[tokio::test]
    async fn over_claim_space() {
        let res = SimpleResourceProvider::new(2, 10);
        let mut claim = res.claim(10).await.unwrap();
        assert_eq!(res.spare_claims().await, 1);
        assert_eq!(res.spare_space().await, 0);
        assert!(res.claim(1).await.is_busy());
        claim.release().await;
    }

    #[tokio::test]
    async fn over_claim_count() {
        let res = SimpleResourceProvider::new(1, 10);
        let mut claim = res.claim(1).await.unwrap();
        assert_eq!(res.spare_claims().await, 0);
        assert_eq!(res.spare_space().await, 9);
        assert!(res.claim(1).await.is_busy());
        claim.release().await;
    }

    #[tokio::test]
    async fn manage_claim_huge() {
        let res = SimpleResourceProvider::new(1, 10);
        let mut claim = res.claim(100).await.unwrap();
        claim.release().await;
    }

    #[tokio::test]
    async fn ok_limited_claim_huge() {
        let res = SimpleResourceProvider::new_with_max_space(1, 10, 50);
        let mut claim = res.claim(50).await.unwrap();
        claim.release().await;
    }

    #[tokio::test]
    async fn prevented_limited_claim_huge() {
        let res = SimpleResourceProvider::new_with_max_space(1, 10, 50);
        assert!(res.claim(100).await.is_impossible());
    }

    #[throws(tokio::io::Error)]
    async fn get_tempdir() -> TempDir {
        let mut base_path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
        base_path.push("testing");
        fs::create_dir_all(&base_path).await?;
        TempDir::new_in(base_path)?
    }

    #[throws(tokio::io::Error)]
    async fn generate_testdir() -> TempDir {
        let tdir = get_tempdir().await?;
        let base_path = tdir.path();

        // Prepare a very simple directory structure of stuff
        fs::create_dir(base_path.join("bin")).await?;
        fs::create_dir(base_path.join("lib")).await?;
        fs::create_dir(base_path.join("share")).await?;
        fs::create_dir(base_path.join("share").join("doc")).await?;
        // And populate it with a number of files
        fs::write(base_path.join("README"), "This is the README file\n").await?;
        fs::write(
            base_path.join("share/doc/README"),
            "This is the README file\n",
        )
        .await?;
        fs::write(base_path.join("bin/program"), "This is a program file\n").await?;
        fs::write(
            base_path.join("bin/program2"),
            "This is another program file\n",
        )
        .await?;
        tdir
    }

    #[tokio::test]
    async fn check_import_stream() {
        let tdir = generate_testdir().await.unwrap();
        let mut fstream = FSImportStream::new(tdir.path())
            .await
            .unwrap()
            .into_stream();
        // Verify that we meet the rules of the stream
        let mut dirs = Vec::new();
        let mut files = Vec::new();
        let mut expecting_data = false;
        while let Some(event) = fstream.next().await {
            match event {
                ImportEvent::Error(e) => panic!("{:?}", e),
                ImportEvent::Directory(d) => {
                    if expecting_data {
                        panic!("Got directory {:?} when expecting file data", d);
                    }
                    dirs.push(d);
                }
                ImportEvent::File(pd, fname, size, exec) => {
                    if expecting_data {
                        panic!("Got file entry when expecting file data!");
                    }
                    files.push((pd, fname, size, exec, None));
                    expecting_data = true;
                }
                ImportEvent::FileData(d) => {
                    if !expecting_data {
                        panic!("Got unexpected file data");
                    }
                    let lastidx = files.len() - 1;
                    files[lastidx].4 = Some(d);
                    expecting_data = false;
                }
            }
        }
        // Next verify that certain dirs are present etc.
        // There should be `bin` `lib` `share` and `share/doc`
        assert!(dirs.contains(&PathBuf::from("bin")));
        assert!(dirs.contains(&PathBuf::from("lib")));
        assert!(dirs.contains(&PathBuf::from("share")));
        assert!(dirs.contains(&PathBuf::from("share/doc")));
        // Next verify that every file has data
        for f in &files {
            assert!(f.4.is_some());
        }
    }

    #[tokio::test(threaded_scheduler)]
    async fn verify_importing() {
        let tdir = generate_testdir().await.unwrap();
        let fstream = FSImportStream::new(tdir.path())
            .await
            .unwrap()
            .into_stream();
        let storage_dir = get_tempdir().await.unwrap();
        let mut storage = crate::SharedStorage::new(storage_dir.path()).await.unwrap();
        let mut linear_loader = crate::util::SimpleResourceProvider::new(1, 1);
        storage
            .import("test-index-1", &mut linear_loader, fstream)
            .await
            .unwrap();
        println!("Made it to the end");
    }
}

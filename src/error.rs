//! Internal module, not exported, see crate docs
//!

use thiserror::Error;

use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Error, Debug)]
pub enum Error {
    #[error("threading error of some kind")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("error while preparing store")]
    Preparing(std::io::Error),
    #[error("entry {0:?} not found in storage index")]
    EntryNotFound(OsString),
    #[error("entry {0:?} was not a directory when traversing storage index")]
    EntryNotDirectory(OsString),
    #[error("unexpected prefix component encountered when traversing storage index for {0:?}")]
    UnexpectedPrefix(PathBuf),
    #[error(
        "unexpected parent directory component encountered when traversing storage index for {0:?}"
    )]
    UnexpectedParent(PathBuf),
    #[error(
        "unexpected root directory component encountered when traversing storage index for {0:?}"
    )]
    UnexpectedRootDir(PathBuf),
    #[error("index {0:?} too large ({1} bytes)")]
    IndexTooLarge(PathBuf, u64),
    #[error("error parsing index")]
    ParsingIndex(serde_json::Error),
    #[error("serialising index")]
    SerialisingIndex(serde_json::Error),
    #[error("error while writing index file")]
    WritingIndex(PathBuf, std::io::Error),
    #[error("file data packet out of order when unpacking into storage")]
    UnexpectedFileData,
    #[error("entry exists as directory when trying to insert {0:?}")]
    FileEntryExistsAsDirectory(PathBuf),
    #[error("entry exists as different file when trying to insert {0:?}")]
    FileEntryExistsAsFile(PathBuf),
    #[error("entry exists as a file when trying to make directory {0:?}")]
    DirectoryEntryExistsAsFile(PathBuf),
    #[error("attempted to import a file too large for resource provider {0:?} is {1} bytes")]
    ImpossibleFileClaim(PathBuf, usize),
    #[error("unexpected end of content when unpacking into storage")]
    UnexpectedEndOfContent,
    #[error("expected file data event, got something else when unpacking into storage")]
    ExpectedFileDataEvent,
    #[error("IO error while adding entry {0:?} into storage: {1:?}")]
    IOErrorAddingToStorage(PathBuf, std::io::Error),
}

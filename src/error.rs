//! Internal module, not exported, see crate docs
//!

use thiserror::Error;

use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Error, Debug)]
pub enum Error {
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
}

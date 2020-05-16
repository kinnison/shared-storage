//! # Shared storage
//!
//! Shared storage is a storage model akin to, though not identical in semantics,
//! Google's ContentAddressableStorage model in their RemoteExecution API.
//!
//! A shared storage contains some amount of data files, identified by their
//! SHA-256 sum, length, and whether or not they are considered executable.
//! (Note, on Windows, executable/not-executable is effectively ignored).
//! In addition, shared storage has a concept of indices which are treelike
//! structures akin to a filesystem heirarchy consisting entirely of directories
//! and file entries, which map names to the data files above.  Indices are given
//! names, and while an index is present in a shared storage, the entries it
//! references will not be removed from the storage.  Removing an index from
//! the shared storage model may result in space being freed up.
//!
//! Shared storages are populated by importing tarballs to create indices.  Indices
//! can be merged to form new indices, and storages are depopulated by removing
//! indices.
//!
//! Shared storage is meant to be used in an asynchronous situation and so uses
//! tokio for all its filesystem accesses.

mod error;
pub use error::Error;

#[macro_use]
mod traits;
pub use traits::{ResourceAllocation, ResourceClaimResult, ResourceProvider};

pub mod entry;
pub mod storage;

pub mod util;

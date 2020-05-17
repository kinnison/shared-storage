//! Entries in a storage index form a heirarchy of directories and
//! file entries (whose content is in the shared storage)

use fehler::{throw, throws};
use serde::{Deserialize, Serialize};

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::default::Default;
use std::ffi::{OsStr, OsString};
use std::path::Path;

use crate::storage::StorageIdentifier;
use crate::Error;

#[derive(Debug, Serialize, Deserialize)]
pub enum DirectoryEntry {
    Directory(Directory),
    File(StorageIdentifier),
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Directory {
    entries: HashMap<OsString, DirectoryEntry>,
}

impl Directory {
    #[throws(Error)]
    fn descend<C: AsRef<OsStr>>(&self, component: C) -> &Directory {
        let component = component.as_ref();
        if !self.entries.contains_key(component) {
            throw!(Error::EntryNotFound(component.into()));
        }
        match &self.entries[component] {
            DirectoryEntry::Directory(d) => d,
            _ => throw!(Error::EntryNotDirectory(component.into())),
        }
    }

    #[throws(Error)]
    fn descend_mut<C: AsRef<OsStr>>(&mut self, component: C, create: bool) -> &mut Directory {
        let component = component.as_ref();
        if !self.entries.contains_key(component) {
            if create {
                self.entries.insert(
                    component.to_owned(),
                    DirectoryEntry::Directory(Directory::default()),
                );
            } else {
                throw!(Error::EntryNotFound(component.into()));
            }
        }
        // Unwrap is fine because we ensured it exists above
        match self.entries.get_mut(component).unwrap() {
            DirectoryEntry::Directory(d) => d,
            _ => throw!(Error::EntryNotDirectory(component.into())),
        }
    }

    #[throws(Error)]
    pub fn traverse<P: AsRef<Path>>(&self, path: P) -> &Directory {
        let path = path.as_ref();
        let mut here = self;
        for component in path.components() {
            use std::path::Component;
            match component {
                Component::CurDir => {}
                Component::Prefix(_) => throw!(Error::UnexpectedPrefix(path.into())),
                Component::ParentDir => throw!(Error::UnexpectedParent(path.into())),
                Component::RootDir => throw!(Error::UnexpectedRootDir(path.into())),
                Component::Normal(c) => here = here.descend(c)?,
            }
        }
        here
    }

    #[throws(Error)]
    pub fn traverse_mut<P: AsRef<Path>>(&mut self, path: P, create: bool) -> &mut Directory {
        let path = path.as_ref();
        let mut here = self;
        for component in path.components() {
            use std::path::Component;
            match component {
                Component::CurDir => {}
                Component::Prefix(_) => throw!(Error::UnexpectedPrefix(path.into())),
                Component::ParentDir => throw!(Error::UnexpectedParent(path.into())),
                Component::RootDir => throw!(Error::UnexpectedRootDir(path.into())),
                Component::Normal(c) => here = here.descend_mut(c, create)?,
            }
        }
        here
    }

    #[throws(Error)]
    pub fn insert_file<S: Into<OsString>>(&mut self, file_name: S, identity: StorageIdentifier) {
        let file_name = file_name.into();
        match self.entries.entry(file_name) {
            Entry::Vacant(v) => {
                v.insert(DirectoryEntry::File(identity));
            }
            Entry::Occupied(v) => match v.get() {
                DirectoryEntry::Directory(_) => {
                    throw!(Error::FileEntryExistsAsDirectory(v.key().into()))
                }
                DirectoryEntry::File(f) if f != &identity => {
                    throw!(Error::FileEntryExistsAsFile(v.key().into()))
                }
                _ => {}
            },
        }
    }

    #[throws(Error)]
    pub fn mkdir<S: Into<OsString>>(&mut self, file_name: S) {
        let file_name = file_name.into();
        match self.entries.entry(file_name) {
            Entry::Vacant(v) => {
                v.insert(DirectoryEntry::Directory(Directory::default()));
            }
            Entry::Occupied(v) => match v.get() {
                DirectoryEntry::File(_) => {
                    throw!(Error::DirectoryEntryExistsAsFile(v.key().into()))
                }
                _ => {}
            },
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl TryFrom<&str> for Directory {
    type Error = json5::Error;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        json5::from_str(s)
    }
}

impl TryFrom<&Directory> for String {
    type Error = json5::Error;
    fn try_from(d: &Directory) -> Result<String, Self::Error> {
        json5::to_string(d)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn empty_dir() {
        let dir = Directory::default();
        assert!(dir.is_empty());
    }
}

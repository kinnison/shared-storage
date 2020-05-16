//! Entries in a storage index form a heirarchy of directories and
//! file entries (whose content is in the shared storage)

use fehler::{throw, throws};
use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
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
}

impl TryFrom<&str> for Directory {
    type Error = serde_json::Error;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        serde_json::from_str(s)
    }
}

impl TryFrom<&Directory> for String {
    type Error = serde_json::Error;
    fn try_from(d: &Directory) -> Result<String, Self::Error> {
        serde_json::to_string(d)
    }
}

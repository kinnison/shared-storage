//! Traits used by shared-storage
//!

use async_trait::async_trait;

use std::matches;

pub enum ResourceClaimResult<T> {
    Ok(T),
    Busy,
    Impossible,
}

impl<T> ResourceClaimResult<T> {
    pub fn unwrap(self) -> T {
        match self {
            Self::Ok(c) => c,
            Self::Busy => panic!("Attempt to unwrap a busy claim"),
            Self::Impossible => panic!("Attempt to unwrap an impossible claim"),
        }
    }

    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok(_))
    }

    pub fn is_busy(&self) -> bool {
        matches!(self, Self::Busy)
    }

    pub fn is_impossible(&self) -> bool {
        matches!(self, Self::Impossible)
    }
}

#[async_trait]
pub trait ResourceProvider {
    /// The type of a claimed resource, basically opaque to caller
    /// It ought to implement Send and Sync or it'll probably go boom
    /// It must implement ResourceAllocation or the claim function won't work
    /// For catching of programming errors, you should probably use the
    /// drop_claim_impl macro for your impl
    type ResourceClaim;
    /// Attempt to claim size bytes.  If size is 0 this could
    /// still fail if the resource provider is limiting number of active claims
    async fn claim(&self, size: usize) -> ResourceClaimResult<Self::ResourceClaim>
    where
        Self::ResourceClaim: ResourceAllocation;

    /// How many claims are active
    /// Note, this information might be out of date by the time it returns
    async fn claims_in_use(&self) -> usize;

    /// How many claims are spare
    /// Note, this information might be out of date by the time it returns
    async fn spare_claims(&self) -> usize;

    /// How much RAM in active claims
    /// Note, this information might be out of date by the time it returns
    async fn space_in_use(&self) -> usize;

    /// How much RAM is spare
    /// Note, space_in_use() + spare_space() does necessarily indicate max space
    /// which could be claimed, it is merely an indication
    /// Note, this information might be out of date by the time it returns
    async fn spare_space(&self) -> usize;
}

#[async_trait]
pub trait ResourceAllocation: Send + Sync {
    /// Release the allocation, failure to call this will result in either a panic
    /// at runtime when the allocation is dropped most likely, or else a resource
    /// leak.  Some implementations maybe
    async fn release(&mut self);

    /// Synchronously test if the allocation was already released
    fn released(&self) -> bool;
}

#[macro_export]
macro_rules! drop_claim_impl {
    ($t:ty) => {
        impl Drop for $t {
            fn drop(&mut self) {
                if !self.released() {
                    panic!("Dropped unreleased claim")
                }
            }
        }
    };
}

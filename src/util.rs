//! Useful utility stuff for shared storage
//!

use async_trait::async_trait;
use tokio::sync::Mutex;

use std::sync::Arc;

use crate::{ResourceAllocation, ResourceClaimResult, ResourceProvider};

type AMSRPInner = Arc<Mutex<SRPInner>>;

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
    pub fn new(claims: usize, space: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SRPInner {
                claims,
                space,
                ..SRPInner::default()
            })),
        }
    }

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

#[cfg(test)]
mod test {
    use super::*;

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
}

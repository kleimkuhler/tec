#[macro_use]
extern crate futures;
extern crate tokio;
extern crate tokio_timer;

use futures::{Async, Future, Poll, Stream};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::timer::{delay_queue, DelayQueue, Error, Interval};

struct Cache {
    inner: Arc<Mutex<CacheInner>>,
}

struct CacheInner {
    capacity: usize,
    expires: Duration,
    expirations: DelayQueue<usize>,
    vals: HashMap<usize, Node>,
}

struct PurgeCache {
    inner: Weak<Mutex<CacheInner>>,
    interval: Interval,
}

struct Access<'a> {
    expirations: &'a mut DelayQueue<usize>,
    node: &'a mut Node,
}

struct Node {
    expires: Duration,
    key: delay_queue::Key,
    value: usize,
}

#[derive(Debug, PartialEq)]
struct CapacityExhausted {
    capacity: usize,
}

struct Reserve<'a> {
    expirations: &'a mut DelayQueue<usize>,
    expires: Duration,
    vals: &'a mut HashMap<usize, Node>,
}

// ===== impl Cache =====

impl Cache {
    fn new(capacity: usize, expires: Duration) -> (Arc<Mutex<CacheInner>>, PurgeCache) {
        let inner = Arc::new(Mutex::new(CacheInner {
            capacity,
            expires,
            expirations: DelayQueue::with_capacity(capacity),
            vals: HashMap::default(),
        }));
        let purge_cache = PurgeCache {
            inner: Arc::downgrade(&inner),
            interval: Interval::new_interval(expires),
        };

        (inner, purge_cache)
    }
}

// ===== impl CacheInner =====

impl CacheInner {
    fn access(&mut self, key: &usize) -> Option<Access> {
        let node = self.vals.get_mut(key)?;
        Some(Access {
            expirations: &mut self.expirations,
            node,
        })
    }

    fn reserve(&mut self) -> Result<Reserve, CapacityExhausted> {
        if self.vals.len() == self.capacity {
            match self.expirations.poll() {
                Ok(Async::Ready(Some(entry))) => {
                    self.vals.remove(entry.get_ref());
                }

                _ => {
                    return Err(CapacityExhausted {
                        capacity: self.capacity,
                    });
                }
            }
        }

        Ok(Reserve {
            expirations: &mut self.expirations,
            expires: self.expires,
            vals: &mut self.vals,
        })
    }

    fn poll_purge(&mut self) -> Poll<(), Error> {
        while let Some(entry) = try_ready!(self.expirations.poll()) {
            self.vals.remove(entry.get_ref());
        }

        Ok(Async::Ready(()))
    }
}

// ===== impl Access =====

impl<'a> Drop for Access<'a> {
    fn drop(&mut self) {
        self.expirations.reset(&self.node.key, self.node.expires);
    }
}

// ===== impl Node =====

impl Node {
    fn new(expires: Duration, key: delay_queue::Key, value: usize) -> Self {
        Node {
            expires,
            key,
            value,
        }
    }
}

// ===== impl Reserve =====

impl<'a> Reserve<'a> {
    fn store(self, key: usize, val: usize) {
        let node = {
            let delay = self.expirations.insert(key, self.expires);
            Node::new(self.expires, delay, val)
        };
        self.vals.insert(key, node);
    }
}

// ===== impl PurgeCache =====

impl Future for PurgeCache {
    type Item = ();

    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        try_ready!(self.interval.poll().map_err(|_| ()));

        let lock = match self.inner.upgrade() {
            Some(lock) => lock,
            None => return Ok(Async::Ready(())),
        };
        let mut inner = lock.lock().unwrap();

        inner.poll_purge().map_err(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_and_store() {
        let (cache, _cache_purge) = Cache::new(2, Duration::from_secs(1));

        let mut inner = cache.lock().unwrap();

        inner.reserve().expect("reserve").store(1, 2);
        assert_eq!(inner.vals.len(), 1);

        inner.reserve().expect("reserve").store(2, 3);
        assert_eq!(inner.vals.len(), 2);

        assert_eq!(
            inner.reserve().err(),
            Some(CapacityExhausted { capacity: 2 })
        );

        assert_eq!(inner.vals.len(), 2);
    }

    #[test]
    fn store_and_access() {
        let (cache, _cache_purge) = Cache::new(2, Duration::from_secs(1));

        let mut inner = cache.lock().unwrap();

        assert!(inner.access(&1).is_none());
        assert!(inner.access(&2).is_none());

        inner.reserve().expect("reserve").store(1, 2);
        assert!(inner.access(&1).is_some());
        assert!(inner.access(&2).is_none());

        inner.reserve().expect("reserve").store(2, 3);
        assert!(inner.access(&1).is_some());
        assert!(inner.access(&2).is_some());
    }

    #[test]
    fn reserve_does_nothing_when_capacity_exists() {
        let (cache, _cache_purge) = Cache::new(2, Duration::from_secs(1));

        let mut inner = cache.lock().unwrap();

        inner.reserve().expect("capacity").store(1, 2);
        assert_eq!(inner.vals.len(), 1);

        assert!(inner.reserve().is_ok());
        assert_eq!(inner.vals.len(), 1);
    }

    #[test]
    fn it_actually_works() {
        tokio::run(futures::lazy(|| {
            let (cache, cache_purge) = Cache::new(2, Duration::from_secs(1));

            tokio::spawn(cache_purge);

            {
                let mut inner = cache.lock().unwrap();

                inner.reserve().expect("reserve").store(1, 2);
                assert_eq!(inner.vals.len(), 1);
            }

            tokio::spawn(
                tokio_timer::sleep(Duration::from_secs(3))
                    .map(move |_| {
                        assert_eq!(cache.lock().unwrap().vals.len(), 0);
                    })
                    .map_err(|_| ()),
            );

            Ok(())
        }))
    }
}

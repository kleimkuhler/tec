extern crate tokio;
extern crate tokio_timer;

use futures::{Async, Future, Poll, Stream};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, TryLockError, Weak},
    time::Duration,
};
use tokio::timer::{delay_queue, DelayQueue, Error, Interval};

/// A generic container that contains a cache.
///
/// An example of what a container may represent is a Router that contains
/// a cache of recently used services.
pub struct Container {
    cache: Arc<Mutex<Cache>>,
}

/// An LRU cache that is purged by a background purge task.
pub struct Cache {
    capacity: usize,
    expires: Duration,
    expirations: DelayQueue<usize>,
    vals: HashMap<usize, Node>,
}

/// Purge a cache at a set interval.
pub struct PurgeCache {
    cache: Weak<Mutex<Cache>>,
    interval: Interval,
}

/// Wrap a cache node so that a lock is held on the entire cache. When the
/// access is dropped, reset the cache node so that it is not purged.
pub struct Access<'a> {
    expirations: &'a mut DelayQueue<usize>,
    node: &'a mut Node,
}

/// This is the handle to a cache value.
pub struct Node {
    expires: Duration,
    key: delay_queue::Key,
    value: usize,
}

#[derive(Debug, PartialEq)]
pub struct CapacityExhausted {
    capacity: usize,
}

/// A handle to a cache that has capacity for at least one additional value.
pub struct Reserve<'a> {
    expirations: &'a mut DelayQueue<usize>,
    expires: Duration,
    vals: &'a mut HashMap<usize, Node>,
}

// ===== impl Container =====

impl Container {
    pub fn new(capacity: usize, expires: Duration) -> (Self, PurgeCache) {
        let container = Self {
            cache: Arc::new(Mutex::new(Cache::new(capacity, expires))),
        };
        let purge_cache = PurgeCache {
            cache: Arc::downgrade(&container.cache),
            interval: Interval::new_interval(expires),
        };

        (container, purge_cache)
    }
}

// ===== impl Cache =====

impl Cache {
    pub fn new(capacity: usize, expires: Duration) -> Self {
        Self {
            capacity,
            expires,
            expirations: DelayQueue::with_capacity(capacity),
            vals: HashMap::default(),
        }
    }

    pub fn access(&mut self, key: &usize) -> Option<Access> {
        let node = self.vals.get_mut(key)?;
        Some(Access {
            expirations: &mut self.expirations,
            node,
        })
    }

    pub fn reserve(&mut self) -> Result<Reserve, CapacityExhausted> {
        if self.vals.len() == self.capacity {
            match self.expirations.poll() {
                // The cache is at capacity but we are able to remove a value.
                Ok(Async::Ready(Some(entry))) => {
                    self.vals.remove(entry.get_ref());
                }

                // `Ready(None)` can only be returned when expirations is
                // empty. We know `expirations` is not empty because `vals` is
                // not empty.
                Ok(Async::Ready(None)) => unreachable!(),

                // The cache is at capacity and no values can be removed.
                Ok(Async::NotReady) => {
                    return Err(CapacityExhausted {
                        capacity: self.capacity,
                    });
                }

                // `warn!` could be helpful here in an actual use case.
                Err(_e) => {
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

// ===== impl PurgeCache =====

impl Future for PurgeCache {
    type Item = ();

    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        try_ready!(self.interval.poll().map_err(|e| {
            println!("error polling purge interval: {:?}", e);
            ()
        }));

        let lock = match self.cache.upgrade() {
            Some(lock) => lock,
            None => return Ok(Async::Ready(())),
        };
        let mut cache = match lock.try_lock() {
            Ok(lock) => lock,
            Err(TryLockError::Poisoned(e)) => panic!("lock poisoned: {:?}", e),
            Err(_) => {
                futures::task::current().notify();
                return Ok(Async::NotReady);
            }
        };

        cache.poll_purge().map_err(|e| {
            println!("error purging cache: {:?}", e);
            ()
        })
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
    pub fn new(expires: Duration, key: delay_queue::Key, value: usize) -> Self {
        Node {
            expires,
            key,
            value,
        }
    }
}

// ===== impl Reserve =====

impl<'a> Reserve<'a> {
    pub fn store(self, key: usize, val: usize) {
        let node = {
            let delay = self.expirations.insert(key, self.expires);
            Node::new(self.expires, delay, val)
        };
        self.vals.insert(key, node);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_and_store() {
        let (container, _cache_purge) = Container::new(2, Duration::from_millis(10));

        let mut cache = container.cache.lock().unwrap();

        cache.reserve().expect("reserve").store(1, 2);
        assert_eq!(cache.vals.len(), 1);

        cache.reserve().expect("reserve").store(2, 3);
        assert_eq!(cache.vals.len(), 2);

        assert_eq!(
            cache.reserve().err(),
            Some(CapacityExhausted { capacity: 2 })
        );

        assert_eq!(cache.vals.len(), 2);
    }

    #[test]
    fn store_access_value() {
        let (container, _cache_purge) = Container::new(2, Duration::from_millis(10));

        let mut cache = container.cache.lock().unwrap();

        assert!(cache.access(&1).is_none());
        assert!(cache.access(&2).is_none());

        cache.reserve().expect("reserve").store(1, 2);
        assert!(cache.access(&1).is_some());
        assert!(cache.access(&2).is_none());

        cache.reserve().expect("reserve").store(2, 3);
        assert!(cache.access(&1).is_some());
        assert!(cache.access(&2).is_some());
    }

    #[test]
    fn reserve_does_nothing_when_capacity_exists() {
        let (container, _cache_purge) = Container::new(2, Duration::from_millis(10));

        let mut cache = container.cache.lock().unwrap();

        cache.reserve().expect("capacity").store(1, 2);
        assert_eq!(cache.vals.len(), 1);

        assert!(cache.reserve().is_ok());
        assert_eq!(cache.vals.len(), 1);
    }

    #[test]
    fn store_and_self_purge() {
        tokio::run(futures::lazy(|| {
            let (container, _cache_purge) = Container::new(2, Duration::from_millis(10));

            {
                let mut cache = container.cache.lock().unwrap();

                cache.reserve().expect("reserve").store(1, 2);
                cache.reserve().expect("reserve").store(2, 3);
                assert_eq!(cache.vals.len(), 2);
            }

            tokio::spawn(
                tokio_timer::sleep(Duration::from_millis(100))
                    .map(move |_| {
                        let mut cache = container.cache.lock().unwrap();

                        cache.reserve().expect("reserve").store(3, 4);
                        assert_eq!(cache.vals.len(), 2);
                        assert!(cache.access(&3).is_some());
                    })
                    .map_err(|_| ()),
            );

            Ok(())
        }))
    }

    #[test]
    fn store_and_background_purge() {
        tokio::run(futures::lazy(|| {
            let (container, cache_purge) = Container::new(2, Duration::from_millis(10));

            tokio::spawn(cache_purge);

            {
                let mut cache = container.cache.lock().unwrap();

                cache.reserve().expect("reserve").store(1, 2);
                cache.reserve().expect("reserve").store(2, 3);
                assert_eq!(cache.vals.len(), 2);
            }

            tokio::spawn(
                tokio_timer::sleep(Duration::from_millis(100))
                    .map(move |_| {
                        assert_eq!(container.cache.lock().unwrap().vals.len(), 0);
                    })
                    .map_err(|_| ()),
            );

            Ok(())
        }))
    }

    #[test]
    fn drop_access_resets_expiration() {
        use tokio::runtime::current_thread::Runtime;

        let mut runtime = Runtime::new().unwrap();

        let (container, cache_purge) = Container::new(2, Duration::from_millis(10));

        runtime.spawn(cache_purge);

        let mut cache = container.cache.lock().unwrap();

        cache.reserve().expect("reserve").store(1, 2);
        assert!(cache.access(&1).is_some());
        let access = cache.access(&1);

        runtime
            .block_on(tokio_timer::sleep(Duration::from_millis(100)))
            .unwrap();

        drop(access);
        drop(cache);

        runtime
            .block_on(tokio_timer::sleep(Duration::from_millis(1)))
            .unwrap();

        let mut cache = container.cache.lock().unwrap();

        assert!(cache.access(&1).is_some());
    }
}

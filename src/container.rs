/// This mocks a generic container that wraps a cache.
///
/// There are two structs that wrap containers - a `Container` and a
/// `PurgeCache`.
///
/// `Container` can represent some object that would contain a cache, such as a
/// router that maintains a cache of services.
///
/// `PurgeCache` contains a cache and implements `Future` so that it can be
/// polled as a background task and purge values from the cache when it is able
/// to.
///
/// Both `Container` and `PurgeCache` contend for a `Mutex` that wraps a single
/// cache. `Container` has priority to locking the cache. This is ensured by
/// `Container` blocking when acquiring a lock, and `PurgeCache` trying to lock
/// - scheduling itself to be polled in the near future if it fails.
///
/// Access to cache values is maintained through handles to `Access`. `Access`
/// wraps a `Node` and a delay queue `expirations`. `Node` provides access to an
/// actual cache value.
///
/// The reason a handle is provided to the cache value is because there may be
/// ongoing access to the value and we want to make sure it is not purged from
/// the cache by `PurgeCache`. When the handle is dropped, `Access` resets the
/// cache value in the delay queue `expirations`.
///
/// Similarly, we want to be able to add values to the cache, but the value may
/// not be ready yet. We handle this situation with `Reserve`. `Reserve`
/// represents a handle to a cache that has capacity for at least one additional
/// value.
extern crate tokio;
extern crate tokio_timer;

use futures::{Async, Future, Poll, Stream};
use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Mutex, TryLockError, Weak},
    time::Duration,
};
use tokio::timer::{delay_queue, DelayQueue, Error, Interval};

/// A generic container that contains a cache.
///
/// An example of what a container may represent is a Router that contains
/// a cache of recently used services.
pub struct Container<K: Clone + Eq + Hash, V> {
    cache: Arc<Mutex<Cache<K, V>>>,
}

/// An LRU cache that is purged by a background purge task.
pub struct Cache<K: Clone + Eq + Hash, V> {
    capacity: usize,
    expires: Duration,
    expirations: DelayQueue<K>,
    vals: HashMap<K, Node<V>>,
}

/// Purge a cache at a set interval.
pub struct PurgeCache<K: Clone + Eq + Hash, V> {
    cache: Weak<Mutex<Cache<K, V>>>,
    interval: Interval,
}

/// Wrap a cache node so that a lock is held on the entire cache. When the
/// access is dropped, reset the cache node so that it is not purged.
pub struct Access<'a, K: Clone + Eq + Hash, V> {
    expires: Duration,
    expirations: &'a mut DelayQueue<K>,
    node: &'a mut Node<V>,
}

/// This is the handle to a cache value.
pub struct Node<T> {
    key: delay_queue::Key,
    value: T,
}

#[derive(Debug, PartialEq)]
pub struct CapacityExhausted {
    capacity: usize,
}

/// A handle to a cache that has capacity for at least one additional value.
pub struct Reserve<'a, K: Clone + Eq + Hash, V> {
    expirations: &'a mut DelayQueue<K>,
    expires: Duration,
    vals: &'a mut HashMap<K, Node<V>>,
}

// ===== impl Container =====

impl<K: Clone + Eq + Hash, V> Container<K, V> {
    pub fn new(capacity: usize, expires: Duration) -> (Self, PurgeCache<K, V>) {
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

impl<K: Clone + Eq + Hash, V> Cache<K, V> {
    pub fn new(capacity: usize, expires: Duration) -> Self {
        Self {
            capacity,
            expires,
            expirations: DelayQueue::with_capacity(capacity),
            vals: HashMap::default(),
        }
    }

    pub fn access(&mut self, key: &K) -> Option<Access<K, V>> {
        let node = self.vals.get_mut(key)?;
        Some(Access {
            expires: self.expires,
            expirations: &mut self.expirations,
            node,
        })
    }

    pub fn reserve(&mut self) -> Result<Reserve<K, V>, CapacityExhausted> {
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

impl<K: Clone + Eq + Hash, V> Future for PurgeCache<K, V> {
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
            // If we can lock the cache then do so
            Ok(lock) => lock,

            // If we were unable to lock the cache, panic if the cause of error
            // was a poisoned lock
            Err(TryLockError::Poisoned(e)) => panic!("lock poisoned: {:?}", e),

            // If the lock is not poisoned, it is being held by another
            // thread. Schedule this thread to be polled in the near future.
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

impl<'a, K: Clone + Eq + Hash, V> Drop for Access<'a, K, V> {
    fn drop(&mut self) {
        self.expirations.reset(&self.node.key, self.expires);
    }
}

// ===== impl Node =====

impl<T> Node<T> {
    pub fn new(key: delay_queue::Key, value: T) -> Self {
        Node { key, value }
    }
}

// ===== impl Reserve =====

impl<'a, K: Clone + Eq + Hash, V> Reserve<'a, K, V> {
    pub fn store(self, key: K, val: V) {
        let node = {
            let delay = self.expirations.insert(key.clone(), self.expires);
            Node::new(delay, val)
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

        assert_eq!(cache.access(&1).take().unwrap().node.value, 2);
        assert_eq!(cache.access(&2).take().unwrap().node.value, 3);
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

            // Do not spawn background purge. Instead, fill the cache and
            // force a manual purge to occur when a value can be expired.
            {
                let mut cache = container.cache.lock().unwrap();

                cache.reserve().expect("reserve").store(1, 2);
                cache.reserve().expect("reserve").store(2, 3);
                assert_eq!(cache.vals.len(), 2);
            }

            // Sleep for an amount of time greater than expiry duration so
            // that a value can be purged.
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

            // Spawn a background purge
            tokio::spawn(cache_purge);

            {
                let mut cache = container.cache.lock().unwrap();

                cache.reserve().expect("reserve").store(1, 2);
                cache.reserve().expect("reserve").store(2, 3);
                assert_eq!(cache.vals.len(), 2);
            }

            // Sleep for an amount of time greater than expiry duration so
            // that the background purge purges the entire cache.
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

        // Spawn a background purge on the current runtime
        runtime.spawn(cache_purge);

        let mut cache = container.cache.lock().unwrap();

        cache.reserve().expect("reserve").store(1, 2);
        assert!(cache.access(&1).is_some());
        let access = cache.access(&1);

        // Sleep for an amount of time greater than the expiry duration so
        // that the background purge would purge the cache iff values can
        // be expired.
        runtime
            .block_on(tokio_timer::sleep(Duration::from_millis(100)))
            .unwrap();

        // Explicity drop both the access and cache handles. Dropping the
        // access handle will reset the expiration on the value in the cache.
        // Dropping the cache handle will unlock the cache and allow a
        // background purge to occur.
        drop(access);
        drop(cache);

        // Ensure a background purge is polled so that it can expire any
        // values.
        runtime
            .block_on(tokio_timer::sleep(Duration::from_millis(1)))
            .unwrap();

        let mut cache = container.cache.lock().unwrap();

        // The cache value should still be present since it was reset after
        // the value expiration. We ensured a background purge occurred but
        // that it did not purge the value.
        assert!(cache.access(&1).is_some());
    }
}

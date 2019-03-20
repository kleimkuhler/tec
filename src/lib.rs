#![allow(dead_code)]

#[macro_use]
extern crate futures;
extern crate tokio;
extern crate tokio_timer;

use futures::{Async, Poll, Stream};
use std::{collections::HashMap, sync::Mutex, time::Duration};
use tokio::timer::{delay_queue, DelayQueue, Error};

struct Cache {
    capacity: usize,
    expirations: Mutex<DelayQueue<usize>>,
    expires: Duration,
    vals: HashMap<usize, Node>,
}

struct Access<'a> {
    expirations: &'a Mutex<DelayQueue<usize>>,
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
    expirations: &'a Mutex<DelayQueue<usize>>,
    expires: Duration,
    vals: &'a mut HashMap<usize, Node>,
}

// ===== impl Cache =====

impl Cache {
    fn new(capacity: usize, expires: Duration) -> Self {
        Self {
            capacity,
            expirations: Mutex::new(DelayQueue::with_capacity(capacity)),
            expires,
            vals: HashMap::default(),
        }
    }
}

impl Cache {
    fn access(&mut self, key: &usize) -> Option<Access> {
        let node = self.vals.get_mut(key)?;
        Some(Access {
            expirations: &self.expirations,
            node,
        })
    }

    fn reserve(&mut self) -> Result<Reserve, CapacityExhausted> {
        if self.vals.len() == self.capacity {
            let mut expirations = self.expirations.lock().unwrap();

            match expirations.poll() {
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
            expirations: &self.expirations,
            expires: self.expires,
            vals: &mut self.vals,
        })
    }

    fn poll_purge(&mut self) -> Poll<(), Error> {
        let mut expirations = self.expirations.lock().unwrap();

        while let Some(entry) = try_ready!(expirations.poll()) {
            self.vals.remove(entry.get_ref());
        }

        Ok(Async::Ready(()))
    }
}

// ===== impl Access =====

impl<'a> Drop for Access<'a> {
    fn drop(&mut self) {
        match self.expirations.lock() {
            Ok(mut expirations) => expirations.reset(&self.node.key, self.node.expires),
            Err(_) => (),
        }
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
            let mut expirations = self.expirations.lock().unwrap();
            let delay = expirations.insert(key, self.expires);
            Node::new(self.expires, delay, val)
        };
        self.vals.insert(key, node);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::Future;

    #[test]
    fn reserve_and_store() {
        let mut cache = Cache::new(2, Duration::from_secs(1));

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
    fn store_and_access() {
        let mut cache = Cache::new(2, Duration::from_secs(1));

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
        let mut cache = Cache::new(2, Duration::from_secs(1));

        cache.reserve().expect("capacity").store(1, 2);
        assert_eq!(cache.vals.len(), 1);

        assert!(cache.reserve().is_ok());
        assert_eq!(cache.vals.len(), 1);
    }

    #[test]
    fn it_actually_works() {
        tokio::run(futures::lazy(|| {
            let mut cache = Cache::new(2, Duration::from_secs(1));

            cache.reserve().expect("reserve").store(1, 2);
            assert_eq!(cache.vals.len(), 1);

            tokio_timer::sleep(Duration::from_secs(2))
                .map(move |_| {
                    let _ = cache.poll_purge();
                    assert_eq!(cache.vals.len(), 0);
                    ()
                })
                .map_err(|e| panic!("timer error: {:?}", e))
        }))
    }
}

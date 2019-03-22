## Timed expiration cache

This mocks a generic container that wraps a cache.

There are two structs that wrap containers -- a `Container` and a
`PurgeCache`.

`Container` can represent some object that would contain a cache, such as a
router that maintains a cache of services.

`PurgeCache` contains a cache and implements `Future` so that it can be
polled as a background task and purge values from the cache when it is able
to.

Both `Container` and `PurgeCache` contend for a `Mutex` that wraps a single
cache. `Container` has priority to locking the cache. This is ensured by
`Container` blocking when acquiring a lock, and `PurgeCache` trying to lock --
scheduling itself to be polled in the near future if it fails.

Access to cache values is maintained through handles to `Access`. `Access`
wraps a `Node` and a delay queue `expirations`. `Node` provides access to an
actual cache value.

The reason a handle is provided to the cache value is because there may be
ongoing access to the value and we want to make sure it is not purged from
the cache by `PurgeCache`. When the handle is dropped, `Access` resets the
cache value in the delay queue `expirations`.

Similarly, we want to be able to add values to the cache, but the value may
not be ready yet. We handle this situation with `Reserve`. `Reserve`
represents a handle to a cache that has capacity for at least one additional
value.
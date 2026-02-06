# Changelog

All notable changes to MicroRedis will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.0.0] - 2026-02-06

### Added

- **100 Redis commands** across all supported data types
- **7 data types**: String, Hash, List, Set, Sorted Set, Stream, HyperLogLog
- **Bitmap operations**: SETBIT, GETBIT, BITCOUNT, BITOP, BITFIELD, BITPOS
- **Pub/Sub** with glob pattern matching (SUBSCRIBE, UNSUBSCRIBE, PUBLISH)
- **Transactions** with optimistic locking (WATCH, MULTI, EXEC, DISCARD)
- **TTL system** with lazy expiry on access + active expiry background task
- **Persistence** via MicroRDB binary snapshot format with CRC32 checksums
- **Flash wear-leveling** for ESP32 persistent storage
- **Memory management** with configurable LRU/random eviction and MAX_KEYS enforcement
- **Auth middleware** with password-based authentication
- **Rate limiting** per-client middleware
- **Async TCP server** supporting up to 8 concurrent clients
- **RESP2 protocol** streaming parser with zero-copy buffer operations
- **Built-in test suite** with 74 unit tests (runs on CPython and MicroPython)
- **CPython compatibility** shims for `time.ticks_ms` / `ticks_diff` / `ticks_add`
- Full compatibility with `redis-cli`, `redis-py`, `ioredis`, and any RESP2 client

### Optimized

- `__slots__` on every class (~40% memory savings per instance)
- Zero-copy RESP parsing with `memoryview` over `bytearray`
- Pre-allocated RESP response constants (OK, PONG, NULL, 0, 1)
- Reservoir sampling for approximate LRU (no key list copies)
- Lazy expiry eliminates full-scan overhead
- Per-connection overhead reduced to ~500 bytes

[1.0.0]: https://github.com/mateuszsury/microredis/releases/tag/v1.0.0

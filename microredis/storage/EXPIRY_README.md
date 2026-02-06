# ExpiryManager Module

Min-heap based TTL management for MicroRedis on ESP32-S3 with MicroPython.

## Overview

The ExpiryManager implements Redis-like probabilistic active expiry checking using a min-heap for efficient key expiration. It works alongside the Storage engine's lazy expiry system to provide both:

- **Lazy Expiry**: Keys are checked when accessed (implemented in Storage)
- **Active Expiry**: Background task periodically removes expired keys (implemented here)

## Features

- **Min-Heap Ordering**: O(1) lookup of nearest expiry, O(log n) insert/remove
- **Lazy Cleanup**: Stale heap entries are filtered during pop, avoiding expensive searches
- **Probabilistic Algorithm**: Redis-like sampling strategy (>25% rule)
- **Async Integration**: Non-blocking background task with configurable interval
- **Memory Efficient**: ~24 bytes per heap entry, automatic stale entry cleanup

## Quick Start

```python
from microredis.storage.engine import Storage
from microredis.storage.expiry import ExpiryManager
import asyncio

# Initialize
storage = Storage()
expiry_mgr = ExpiryManager(storage)

# Set key with TTL
storage.set(b'session:123', b'user_data', px=3600000)  # 1 hour
expiry_mgr.add_expiry(b'session:123', storage._expires[b'session:123'])

# Start background expiry loop
expiry_task = asyncio.create_task(expiry_mgr.run_expiry_loop())

# ... server runs ...

# Cleanup
expiry_task.cancel()
await expiry_task
```

## API Reference

### ExpiryManager(storage)

Create a new expiry manager.

**Parameters**:
- `storage: Storage` - storage engine instance to manage

**Attributes**:
- `_heap: list` - min-heap of (expire_time_ms, key) tuples
- `_storage: Storage` - reference to storage engine
- `_last_check: int` - timestamp of last expiry check

### add_expiry(key, expire_at_ms)

Add key expiry to heap.

**Parameters**:
- `key: bytes` - key to track
- `expire_at_ms: int` - absolute expiry timestamp in milliseconds

**Example**:
```python
expire_time = time.ticks_add(time.ticks_ms(), 5000)  # 5 seconds from now
expiry_mgr.add_expiry(b'mykey', expire_time)
```

### update_expiry(key, expire_at_ms)

Update key expiry timestamp. Old entry becomes stale, new entry is added.

**Parameters**:
- `key: bytes` - key to update
- `expire_at_ms: int` - new absolute expiry timestamp in milliseconds

**Example**:
```python
# Extend TTL
storage.pexpire(b'mykey', 10000)  # 10 seconds
expiry_mgr.update_expiry(b'mykey', storage._expires[b'mykey'])
```

### remove_expiry(key)

Mark key expiry as stale (lazy removal, no-op).

**Parameters**:
- `key: bytes` - key to remove from tracking

**Note**: Entry remains in heap but will be filtered during expire_keys().

**Example**:
```python
storage.persist(b'mykey')  # Remove TTL
expiry_mgr.remove_expiry(b'mykey')  # Lazy cleanup
```

### get_nearest_expiry() -> int | None

Get milliseconds until nearest key expiration. Filters stale entries from heap top.

**Returns**:
- `int` - milliseconds until expiry
- `None` - heap is empty

**Example**:
```python
nearest = expiry_mgr.get_nearest_expiry()
if nearest is not None:
    print(f"Next key expires in {nearest}ms")
```

### expire_keys(max_count=20) -> int

Expire keys using probabilistic algorithm.

**Algorithm**:
1. Sample up to max_count keys from heap (oldest first)
2. Delete expired keys
3. If >25% were expired, repeat
4. Stop when ratio drops or max_count reached

**Parameters**:
- `max_count: int` - maximum keys to check per sample (default 20)

**Returns**:
- `int` - number of keys deleted

**Example**:
```python
# Manual expiry check
deleted = expiry_mgr.expire_keys(max_count=100)
print(f"Deleted {deleted} expired keys")
```

### async run_expiry_loop()

Main async loop for active expiry checking. Runs indefinitely, checking for expired keys every EXPIRY_CHECK_INTERVAL_MS (100ms).

**Example**:
```python
# Start as background task
expiry_task = asyncio.create_task(expiry_mgr.run_expiry_loop())

# ... server runs ...

# Stop gracefully
expiry_task.cancel()
try:
    await expiry_task
except asyncio.CancelledError:
    pass
```

## Integration Patterns

### With Storage Engine

**Option 1: Manual Integration**
```python
storage = Storage()
expiry_mgr = ExpiryManager(storage)

# Set with TTL
storage.set(b'key', b'value', px=1000)
expiry_mgr.add_expiry(b'key', storage._expires[b'key'])

# Update TTL
storage.pexpire(b'key', 5000)
expiry_mgr.update_expiry(b'key', storage._expires[b'key'])

# Remove TTL
storage.persist(b'key')
expiry_mgr.remove_expiry(b'key')
```

**Option 2: Automatic Integration** (recommended)
```python
# Add to Storage class
class Storage:
    __slots__ = (..., '_expiry_mgr')

    def __init__(self):
        # ...
        from microredis.storage.expiry import ExpiryManager
        self._expiry_mgr = ExpiryManager(self)

    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        # ... existing code ...
        if px is not None:
            self._set_expiry_relative_ms(key, px)
            self._expiry_mgr.add_expiry(key, self._expires[key])
```

### With Server

```python
class MicroRedisServer:
    def __init__(self, host='0.0.0.0', port=6379):
        self.storage = Storage()
        # Storage automatically creates ExpiryManager

    async def start(self):
        # Start expiry loop
        self._expiry_task = asyncio.create_task(
            self.storage._expiry_mgr.run_expiry_loop()
        )

    async def stop(self):
        # Stop expiry loop
        self._expiry_task.cancel()
        await self._expiry_task
```

## Configuration

Constants defined in `microredis/core/constants.py`:

```python
EXPIRY_CHECK_INTERVAL_MS = const(100)  # Check every 100ms
EXPIRY_SAMPLE_SIZE = const(20)         # Sample 20 keys per check
```

**Tuning**:
- **Lower interval** → More responsive expiry, higher CPU usage
- **Higher interval** → Lower CPU usage, less responsive expiry
- **Smaller sample** → Less work per check, slower overall expiry
- **Larger sample** → More work per check, faster overall expiry

## Performance

### Time Complexity

| Operation | Complexity | Notes |
|-----------|------------|-------|
| add_expiry | O(log n) | heappush |
| update_expiry | O(log n) | heappush (old becomes stale) |
| remove_expiry | O(1) | No-op (lazy) |
| get_nearest_expiry | O(k) | k = stale entries on top |
| expire_keys | O(m log n) | m = keys checked |

### Space Complexity

- Best case: O(k) where k = keys with TTL
- Worst case: O(2k) with many stale entries
- Typical: ~1.2k (lazy cleanup keeps overhead low)

### Memory Footprint

**Per heap entry**: ~24 bytes
- Tuple object header: 8 bytes
- Integer (expire_time): 8 bytes
- Bytes reference (key): 8 bytes

**Example**: 1000 keys with TTL = ~24KB heap overhead

## Testing

### Run Unit Tests

```bash
python test_expiry_unit.py
```

### Run Integration Example

```bash
python test_expiry_example.py
```

## Documentation

- **EXPIRY_DESIGN.md** - Detailed design document with algorithm explanation
- **EXPIRY_INTEGRATION.md** - Integration guide with code examples
- **test_expiry_unit.py** - Comprehensive unit tests
- **test_expiry_example.py** - Usage examples

## Troubleshooting

### Heap Growing Too Large

If `len(heap) >> len(_expires)`, too many stale entries exist.

**Solution**: Rebuild heap
```python
new_heap = [(t, k) for k, t in storage._expires.items()]
heapq.heapify(new_heap)
expiry_mgr._heap = new_heap
```

### Keys Not Expiring

1. Check if expiry loop is running
2. Verify key is in storage._expires
3. Manually trigger: `expiry_mgr.expire_keys(max_count=1000)`

### High CPU Usage

1. Increase EXPIRY_CHECK_INTERVAL_MS
2. Reduce EXPIRY_SAMPLE_SIZE
3. Add yielding after every N keys

## License

Part of MicroRedis project. See main project README for license details.

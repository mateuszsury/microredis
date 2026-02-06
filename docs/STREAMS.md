# Redis Streams in MicroRedis

Redis Streams implementation for MicroRedis on ESP32-S3 with MicroPython.

## Overview

Redis Streams is an append-only log data structure that enables:
- Time-series data logging (sensor readings, metrics)
- Event sourcing and audit logs
- Message queues and pub/sub patterns
- Activity feeds

**Platform**: ESP32-S3 with MicroPython
**Memory**: Optimized for ~300KB available RAM
**File**: `microredis/commands/streams.py`

## Stream Entry ID Format

Each entry has a unique ID: `<millisecondsTime>-<sequenceNumber>`

**Examples**:
- `1526919030474-0` - First entry at timestamp 1526919030474ms
- `1526919030474-1` - Second entry at same timestamp
- `1526919030475-0` - First entry at next timestamp

**Auto-generation**:
- Use `*` as ID to auto-generate based on current time
- IDs are guaranteed to be monotonically increasing
- Uses `time.time() * 1000` for millisecond precision

## Data Structure

Streams are stored as dictionaries in the storage engine:

```python
{
    'entries': [(id_str, {field: value, ...}), ...],  # Ordered list
    'last_id': '0-0',  # Last inserted ID
    'length': 0,       # Number of entries
}
```

**Memory per entry**: ~100-200 bytes overhead + field data
**Recommended max entries**: 1000 for memory-constrained devices
**Management**: Use XTRIM to limit stream size

## Implemented Commands

### XADD - Add Entry

```python
StreamOperations.xadd(storage, key, entry_id, fields) -> bytes
```

**Arguments**:
- `storage`: Storage engine instance
- `key`: Stream key (bytes)
- `entry_id`: Entry ID (bytes) - use `b'*'` for auto-generate
- `fields`: Dict of field-value pairs (bytes -> bytes)

**Returns**: Generated entry ID (bytes)

**Example**:
```python
# Auto-generate ID
entry_id = StreamOperations.xadd(
    storage,
    b'sensor:temp',
    b'*',  # Auto-generate
    {
        b'temperature': b'23.5',
        b'humidity': b'65',
        b'unit': b'C'
    }
)
# Returns: b'1526919030474-0'

# Explicit ID
entry_id = StreamOperations.xadd(
    storage,
    b'sensor:temp',
    b'1526919030500-0',
    {b'temperature': b'24.0'}
)
```

**Errors**:
- ValueError: If ID is not greater than last ID
- ValueError: If ID format is invalid

### XLEN - Get Stream Length

```python
StreamOperations.xlen(storage, key) -> int
```

**Returns**: Number of entries in stream (0 if doesn't exist)

**Example**:
```python
length = StreamOperations.xlen(storage, b'sensor:temp')
# Returns: 42
```

### XRANGE - Get Range (Oldest to Newest)

```python
StreamOperations.xrange(storage, key, start, end, count=None) -> list
```

**Arguments**:
- `start`: Start ID (inclusive) - use `b'-'` for minimum
- `end`: End ID (inclusive) - use `b'+'` for maximum
- `count`: Optional max entries to return

**Returns**: List of `(id_bytes, {field_bytes: value_bytes})`

**Example**:
```python
# Get all entries
entries = StreamOperations.xrange(storage, b'sensor:temp', b'-', b'+')

# Get range
entries = StreamOperations.xrange(
    storage,
    b'sensor:temp',
    b'1526919030474-0',
    b'1526919030500-0'
)

# Get first 10 entries
entries = StreamOperations.xrange(
    storage,
    b'sensor:temp',
    b'-',
    b'+',
    count=10
)

# Result format:
# [
#     (b'1526919030474-0', {b'temperature': b'23.5', b'humidity': b'65'}),
#     (b'1526919030475-0', {b'temperature': b'23.6', b'humidity': b'66'}),
#     ...
# ]
```

### XREVRANGE - Get Range (Newest to Oldest)

```python
StreamOperations.xrevrange(storage, key, end, start, count=None) -> list
```

**Note**: Arguments are reversed compared to XRANGE (end comes first)

**Example**:
```python
# Get last 5 entries
latest = StreamOperations.xrevrange(
    storage,
    b'sensor:temp',
    b'+',  # End (maximum)
    b'-',  # Start (minimum)
    count=5
)

# Iterate newest to oldest
for entry_id, fields in latest:
    print(f"{entry_id}: {fields[b'temperature']}")
```

### XREAD - Read from Multiple Streams

```python
StreamOperations.xread(storage, streams_dict, count=None, block=None) -> list | None
```

**Arguments**:
- `streams_dict`: Dict mapping stream keys to last IDs `{key: last_id}`
- `count`: Optional max entries per stream
- `block`: Timeout in ms (NOT IMPLEMENTED - always non-blocking)

**Returns**: `[(key, [(id, fields)]), ...]` or None if no new entries

**Example**:
```python
# Read new entries from multiple streams
result = StreamOperations.xread(
    storage,
    {
        b'sensor:temp': b'0-0',      # Get all entries
        b'sensor:humidity': b'1526919030474-0',  # Get entries after this ID
        b'sensor:pressure': b'0-0'
    },
    count=10  # Max 10 entries per stream
)

# Result format:
# [
#     (b'sensor:temp', [
#         (b'1526919030474-0', {b'value': b'23.5'}),
#         (b'1526919030475-0', {b'value': b'23.6'})
#     ]),
#     (b'sensor:humidity', [
#         (b'1526919030480-0', {b'value': b'65'})
#     ])
# ]

if result:
    for stream_key, entries in result:
        print(f"Stream: {stream_key.decode()}")
        for entry_id, fields in entries:
            print(f"  {entry_id.decode()}: {fields}")
```

### XTRIM - Trim Stream to Max Length

```python
StreamOperations.xtrim(storage, key, maxlen, approximate=False) -> int
```

**Arguments**:
- `maxlen`: Maximum number of entries to keep
- `approximate`: Approximate trimming (NOT IMPLEMENTED - always exact)

**Returns**: Number of entries deleted

**Example**:
```python
# Keep only last 100 entries (delete older ones)
deleted = StreamOperations.xtrim(storage, b'sensor:temp', 100)
print(f"Deleted {deleted} old entries")

# Check new length
length = StreamOperations.xlen(storage, b'sensor:temp')
# Returns: 100
```

## Usage Patterns

### IoT Sensor Logging

```python
from microredis.storage.engine import Storage
from microredis.commands.streams import StreamOperations

storage = Storage()

# Log sensor reading
def log_reading(sensor_id, temperature, humidity):
    entry_id = StreamOperations.xadd(
        storage,
        f'sensor:{sensor_id}'.encode(),
        b'*',  # Auto-generate ID
        {
            b'temperature': str(temperature).encode(),
            b'humidity': str(humidity).encode()
        }
    )

    # Auto-trim to keep memory under control
    if StreamOperations.xlen(storage, f'sensor:{sensor_id}'.encode()) > 1000:
        StreamOperations.xtrim(storage, f'sensor:{sensor_id}'.encode(), 1000)

    return entry_id

# Log readings
log_reading('living_room', 23.5, 65)
log_reading('bedroom', 21.0, 60)

# Query latest readings
latest = StreamOperations.xrevrange(
    storage,
    b'sensor:living_room',
    b'+',
    b'-',
    count=10
)
```

### Event Logging

```python
# Log user actions
def log_event(user_id, action, details):
    StreamOperations.xadd(
        storage,
        b'events:user_actions',
        b'*',
        {
            b'user_id': str(user_id).encode(),
            b'action': action.encode(),
            b'details': details.encode()
        }
    )

# Log events
log_event(123, 'login', 'success')
log_event(456, 'purchase', 'item:42')

# Query user's recent actions
events = StreamOperations.xrange(
    storage,
    b'events:user_actions',
    b'-',
    b'+',
    count=50
)
```

### Multi-Stream Monitoring

```python
# Monitor multiple sensors
def monitor_sensors():
    # Track last read IDs
    last_ids = {
        b'sensor:temp': b'0-0',
        b'sensor:humidity': b'0-0',
        b'sensor:pressure': b'0-0'
    }

    while True:
        # Read new entries from all sensors
        result = StreamOperations.xread(storage, last_ids, count=10)

        if result:
            for stream_key, entries in result:
                for entry_id, fields in entries:
                    # Process entry
                    print(f"{stream_key.decode()}: {entry_id.decode()} -> {fields}")

                    # Update last read ID
                    last_ids[stream_key] = entry_id

        time.sleep(1)  # Poll every second
```

## Memory Management

**Critical for ESP32-S3**: Streams grow unbounded without trimming.

### Automatic Trimming Pattern

```python
MAX_ENTRIES = 1000  # Adjust based on available memory

def add_with_trim(storage, key, fields):
    # Add entry
    entry_id = StreamOperations.xadd(storage, key, b'*', fields)

    # Auto-trim if exceeds limit
    length = StreamOperations.xlen(storage, key)
    if length > MAX_ENTRIES:
        StreamOperations.xtrim(storage, key, MAX_ENTRIES)

    return entry_id
```

### Memory Estimates

| Entries | Avg Entry Size | Total Memory |
|---------|----------------|--------------|
| 100     | 150 bytes      | ~15 KB       |
| 500     | 150 bytes      | ~75 KB       |
| 1000    | 150 bytes      | ~150 KB      |
| 5000    | 150 bytes      | ~750 KB      |

**Recommendation**: Keep streams under 1000 entries on ESP32-S3.

### Periodic Cleanup

```python
import time

async def periodic_trim():
    """Background task to trim streams periodically."""
    while True:
        # Trim all sensor streams to 500 entries
        for stream_key in [b'sensor:temp', b'sensor:humidity']:
            if StreamOperations.xlen(storage, stream_key) > 500:
                deleted = StreamOperations.xtrim(storage, stream_key, 500)
                print(f"Trimmed {stream_key.decode()}: {deleted} entries deleted")

        await asyncio.sleep(3600)  # Every hour
```

## Limitations

Current implementation has these limitations:

1. **No Blocking XREAD**: `block` parameter is ignored (always non-blocking)
   - Workaround: Poll with `XREAD` in a loop

2. **No Consumer Groups**: XREADGROUP, XACK, XPENDING not implemented
   - Workaround: Manually track last read ID per consumer

3. **No Approximate Trimming**: `approximate` parameter ignored (always exact)
   - Impact: XTRIM is slower on large streams

4. **No MINID Trimming**: Only MAXLEN strategy supported
   - Workaround: Use XRANGE + DEL for time-based cleanup

5. **No XDEL**: Cannot delete individual entries
   - Workaround: Use XTRIM or replace stream

6. **No XINFO**: Cannot query stream metadata
   - Workaround: Use XLEN and XRANGE for introspection

## Performance Characteristics

**Time Complexity**:
- `XADD`: O(1) - constant time append
- `XLEN`: O(1) - length is cached
- `XRANGE`: O(N) where N = number of entries in range
- `XREVRANGE`: O(N) where N = number of entries in range
- `XREAD`: O(N*M) where N = streams, M = entries per stream
- `XTRIM`: O(K) where K = entries to delete

**Space Complexity**:
- Per stream: O(N) where N = number of entries
- Per entry: ~100-200 bytes overhead + field data

**Best Practices**:
1. Use `COUNT` limit with XRANGE/XREVRANGE for large streams
2. Trim streams regularly to bound memory usage
3. Use meaningful field names (they're stored per entry)
4. Consider batching XADD operations if logging frequently
5. Monitor memory with `gc.mem_free()` on ESP32-S3

## Examples

See complete examples in `examples/streams_example.py`:

1. **Sensor Data Logging** - Basic XADD and XLEN usage
2. **Query Historical Data** - XRANGE and XREVRANGE patterns
3. **Multiple Streams** - XREAD for monitoring multiple sensors
4. **Memory Management** - XTRIM strategies
5. **Real-world IoT** - Complete scenario with auto-trimming

Run examples:
```bash
python examples/streams_example.py
```

## Testing

Run the test suite:
```bash
python test_streams.py
```

Tests cover:
- Basic XADD and XLEN
- XRANGE with various parameters
- XREVRANGE ordering
- XREAD multi-stream reads
- XTRIM memory management
- Error handling (invalid IDs, wrong types)
- Memory efficiency with large streams

## References

- [Redis Streams Official Docs](https://redis.io/docs/data-types/streams/)
- [Redis Streams Tutorial](https://redis.io/docs/data-types/streams-tutorial/)
- MicroRedis Memory Module: `.claude/agent-memory/micropython-senior-dev/MEMORY.md`

## Future Enhancements

Planned for future releases:

- [ ] Consumer groups (XGROUP, XREADGROUP, XACK)
- [ ] Blocking XREAD with timeout
- [ ] XDEL for deleting individual entries
- [ ] XINFO for stream introspection
- [ ] MINID trimming strategy
- [ ] Approximate trimming optimization
- [ ] Stream persistence to flash
- [ ] XPENDING, XCLAIM for consumer group management

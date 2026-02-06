# Redis Streams Quick Start Guide

Get started with Redis Streams in MicroRedis in 5 minutes.

## Installation

Streams are built into MicroRedis. No additional dependencies required.

```python
from microredis.storage.engine import Storage
from microredis.commands.streams import StreamOperations
```

## Basic Usage

### 1. Create Storage and Add Entries

```python
# Initialize storage
storage = Storage()

# Add entry with auto-generated ID
entry_id = StreamOperations.xadd(
    storage,
    b'mystream',           # Stream key
    b'*',                  # Auto-generate ID
    {
        b'field1': b'value1',
        b'field2': b'value2'
    }
)

print(f"Added entry: {entry_id.decode()}")
# Output: Added entry: 1526919030474-0
```

### 2. Query Entries

```python
# Get all entries
entries = StreamOperations.xrange(storage, b'mystream', b'-', b'+')

for entry_id, fields in entries:
    print(f"{entry_id.decode()}: {fields}")
# Output: 1526919030474-0: {b'field1': b'value1', b'field2': b'value2'}

# Get last 10 entries
latest = StreamOperations.xrevrange(storage, b'mystream', b'+', b'-', count=10)
```

### 3. Get Stream Length

```python
length = StreamOperations.xlen(storage, b'mystream')
print(f"Stream has {length} entries")
```

### 4. Trim to Limit Size

```python
# Keep only last 100 entries
deleted = StreamOperations.xtrim(storage, b'mystream', 100)
print(f"Deleted {deleted} old entries")
```

## Common Patterns

### IoT Sensor Logging

```python
import time

def log_temperature(sensor_id, temp_celsius):
    """Log temperature reading to stream."""
    stream_key = f'sensor:{sensor_id}'.encode()

    # Add reading with timestamp
    entry_id = StreamOperations.xadd(
        storage,
        stream_key,
        b'*',  # Auto-generate ID based on time
        {
            b'temperature': str(temp_celsius).encode(),
            b'unit': b'celsius'
        }
    )

    # Keep only last 1000 readings (memory management)
    if StreamOperations.xlen(storage, stream_key) > 1000:
        StreamOperations.xtrim(storage, stream_key, 1000)

    return entry_id

# Log readings
log_temperature('bedroom', 21.5)
log_temperature('living_room', 23.0)
time.sleep(1)
log_temperature('bedroom', 21.6)
```

### Read New Entries Only

```python
# Track last read ID
last_id = b'0-0'  # Start from beginning

while True:
    # Read entries after last_id
    result = StreamOperations.xread(
        storage,
        {b'sensor:bedroom': last_id},
        count=10
    )

    if result:
        stream_key, entries = result[0]
        for entry_id, fields in entries:
            temp = fields[b'temperature'].decode()
            print(f"New reading: {temp}°C")
            last_id = entry_id  # Update last read

    time.sleep(1)  # Poll every second
```

### Monitor Multiple Streams

```python
# Read from multiple sensors
result = StreamOperations.xread(
    storage,
    {
        b'sensor:temp': b'0-0',
        b'sensor:humidity': b'0-0',
        b'sensor:pressure': b'0-0'
    },
    count=5  # Max 5 per stream
)

if result:
    for stream_key, entries in result:
        print(f"\n{stream_key.decode()}:")
        for entry_id, fields in entries:
            print(f"  {entry_id.decode()}: {fields}")
```

## Memory Management on ESP32-S3

**Critical**: Always limit stream size to avoid running out of memory.

### Pattern 1: Auto-Trim on Add

```python
MAX_ENTRIES = 500  # Adjust based on available RAM

def add_entry(storage, key, fields):
    # Add entry
    entry_id = StreamOperations.xadd(storage, key, b'*', fields)

    # Trim if too large
    if StreamOperations.xlen(storage, key) > MAX_ENTRIES:
        StreamOperations.xtrim(storage, key, MAX_ENTRIES)

    return entry_id
```

### Pattern 2: Periodic Cleanup

```python
import uasyncio as asyncio

async def cleanup_task():
    """Background task to trim streams."""
    while True:
        # Clean up all sensor streams
        for i in range(10):
            stream_key = f'sensor:{i}'.encode()
            if StreamOperations.xlen(storage, stream_key) > 100:
                deleted = StreamOperations.xtrim(storage, stream_key, 100)
                print(f"Trimmed {stream_key.decode()}: -{deleted}")

        await asyncio.sleep(300)  # Every 5 minutes
```

## ID Format and Ordering

Stream IDs are in format: `<milliseconds>-<sequence>`

```python
# IDs are automatically ordered by time
entry1 = StreamOperations.xadd(storage, b'stream', b'*', {b'msg': b'first'})
# Returns: b'1526919030474-0'

entry2 = StreamOperations.xadd(storage, b'stream', b'*', {b'msg': b'second'})
# Returns: b'1526919030475-0' (or same ms with seq=1)

# Manual ID (must be greater than last)
entry3 = StreamOperations.xadd(storage, b'stream', b'1526919031000-0', {b'msg': b'manual'})
# OK - ID is greater

# This will fail:
# StreamOperations.xadd(storage, b'stream', b'1526919030000-0', {b'msg': b'old'})
# ValueError: ID not greater than last
```

## Error Handling

```python
try:
    # Add entry
    entry_id = StreamOperations.xadd(
        storage,
        b'mystream',
        b'*',
        {b'field': b'value'}
    )
except ValueError as e:
    print(f"Error: {e}")
    # Possible errors:
    # - Invalid ID format
    # - ID not greater than last
    # - Wrong key type (not a stream)
```

## Field Data Types

Fields and values are stored as bytes:

```python
# Convert Python types to bytes
StreamOperations.xadd(storage, b'stream', b'*', {
    b'string': b'hello',                      # String literal
    b'number': str(42).encode(),              # Integer to bytes
    b'float': str(3.14).encode(),             # Float to bytes
    b'bool': b'true',                         # Boolean as string
})

# Read and convert back
entries = StreamOperations.xrange(storage, b'stream', b'-', b'+')
entry_id, fields = entries[0]

string_val = fields[b'string'].decode()       # 'hello'
number_val = int(fields[b'number'].decode())  # 42
float_val = float(fields[b'float'].decode())  # 3.14
bool_val = fields[b'bool'].decode() == 'true' # True
```

## Integration with MicroRedis Server

If using MicroRedis as a server, you'll need to add command handlers:

```python
# In your connection handler (microredis/network/connection.py)

from microredis.commands.streams import StreamOperations

# Add command handlers
self._commands['XADD'] = (self._handle_xadd, 4, -1)
self._commands['XLEN'] = (self._handle_xlen, 2, 2)
self._commands['XRANGE'] = (self._handle_xrange, 4, 5)
# ... etc

async def _handle_xadd(self, args):
    """Handle XADD command."""
    key = args[0]
    entry_id = args[1]

    # Parse fields (remaining args are field-value pairs)
    if len(args) < 4 or (len(args) - 2) % 2 != 0:
        return response.error(b'ERR wrong number of arguments')

    fields = {}
    for i in range(2, len(args), 2):
        fields[args[i]] = args[i + 1]

    try:
        result_id = StreamOperations.xadd(self.storage, key, entry_id, fields)
        return response.bulk_string(result_id)
    except ValueError as e:
        return response.error(str(e).encode())
```

## Testing

Test your streams code:

```python
def test_stream():
    storage = Storage()

    # Add entries
    id1 = StreamOperations.xadd(storage, b'test', b'*', {b'a': b'1'})
    id2 = StreamOperations.xadd(storage, b'test', b'*', {b'a': b'2'})

    # Verify length
    assert StreamOperations.xlen(storage, b'test') == 2

    # Verify range
    entries = StreamOperations.xrange(storage, b'test', b'-', b'+')
    assert len(entries) == 2
    assert entries[0][1][b'a'] == b'1'
    assert entries[1][1][b'a'] == b'2'

    # Verify trim
    deleted = StreamOperations.xtrim(storage, b'test', 1)
    assert deleted == 1
    assert StreamOperations.xlen(storage, b'test') == 1

    print("All tests passed!")

test_stream()
```

## Next Steps

- Read full documentation: `docs/STREAMS.md`
- Explore examples: `examples/streams_example.py`
- Run tests: `python test_streams.py`
- Check Redis Streams docs: https://redis.io/docs/data-types/streams/

## Quick Reference

| Command | Purpose | Example |
|---------|---------|---------|
| `XADD` | Add entry | `xadd(storage, b'key', b'*', {b'f': b'v'})` |
| `XLEN` | Get length | `xlen(storage, b'key')` |
| `XRANGE` | Get range | `xrange(storage, b'key', b'-', b'+')` |
| `XREVRANGE` | Get reverse | `xrevrange(storage, b'key', b'+', b'-')` |
| `XREAD` | Read streams | `xread(storage, {b'key': b'0-0'})` |
| `XTRIM` | Limit size | `xtrim(storage, b'key', 100)` |

## Common Gotchas

1. **Memory**: Always use XTRIM on ESP32-S3 (limited RAM)
2. **IDs**: Use `b'*'` for auto-generation, don't hardcode timestamps
3. **Bytes**: All keys, fields, values must be bytes
4. **Ordering**: IDs must be monotonically increasing
5. **Empty Streams**: XRANGE returns `[]` not None for empty streams
6. **XREAD Result**: Returns `None` if no new entries, not empty list

## Support

For issues or questions:
- Check `docs/STREAMS.md` for detailed documentation
- Review `examples/streams_example.py` for working examples
- Run `test_streams.py` to verify your setup
- See project memory: `.claude/agent-memory/micropython-senior-dev/MEMORY.md`

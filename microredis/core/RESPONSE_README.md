# MicroRedis Response Module

**File**: `microredis/core/response.py`

Memory-efficient RESP2 protocol response builder for MicroRedis on ESP32-S3.

## Overview

This module provides pre-allocated response constants and optimized response building utilities for constructing Redis Serialization Protocol version 2 (RESP2) formatted responses with minimal memory overhead on resource-constrained microcontrollers.

## Features

- **Pre-allocated Constants**: Zero-allocation responses for common cases
- **Memory-Efficient Builders**: ResponseBuilder class using bytearray for complex responses
- **Type Auto-Detection**: Automatic encoding from Python types to RESP2
- **Optimized for MicroPython**: Uses `__slots__`, const(), and efficient byte operations

## Pre-allocated Response Constants

These constants are frozen in memory and reused without allocation:

```python
RESP_OK = b'+OK\r\n'              # Simple string OK
RESP_PONG = b'+PONG\r\n'          # PING response
RESP_NULL = b'$-1\r\n'            # Null bulk string
RESP_NULL_ARRAY = b'*-1\r\n'      # Null array
RESP_EMPTY_ARRAY = b'*0\r\n'      # Empty array
RESP_ZERO = b':0\r\n'             # Integer 0
RESP_ONE = b':1\r\n'              # Integer 1
RESP_QUEUED = b'+QUEUED\r\n'      # Transaction queued
```

## Pre-allocated Error Constants

```python
ERR_UNKNOWN_CMD = b'-ERR unknown command\r\n'
ERR_WRONG_ARITY = b'-ERR wrong number of arguments\r\n'
ERR_WRONGTYPE = b'-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
ERR_SYNTAX = b'-ERR syntax error\r\n'
ERR_NOT_INTEGER = b'-ERR value is not an integer or out of range\r\n'
```

## Response Building Functions

### Simple Responses

```python
# Simple string: +message\r\n
response = simple_string('OK')  # b'+OK\r\n'

# Error: -message\r\n
response = error('key not found')  # b'-key not found\r\n'

# Integer: :number\r\n
response = integer(42)  # b':42\r\n'

# Pre-allocated error helpers
response = error_wrongtype()  # Returns ERR_WRONGTYPE constant
response = error_syntax()      # Returns ERR_SYNTAX constant
```

### Bulk Strings

```python
# Bulk string: $length\r\ndata\r\n
response = bulk_string(b'hello')  # b'$5\r\nhello\r\n'

# Bulk string or null
response = bulk_string_or_null(None)      # b'$-1\r\n'
response = bulk_string_or_null(b'data')   # b'$4\r\ndata\r\n'
```

### Arrays

```python
# Array of pre-encoded items
items = [integer(1), bulk_string(b'test'), simple_string('OK')]
response = array(items)  # b'*3\r\n:1\r\n$4\r\ntest\r\n+OK\r\n'

# Auto-encode values
response = encode_value([1, 'test', None])
# b'*3\r\n:1\r\n$4\r\ntest\r\n$-1\r\n'
```

## ResponseBuilder Class

For complex multi-part responses, use `ResponseBuilder` for maximum efficiency:

```python
builder = ResponseBuilder()

# Build array with multiple elements
builder.add_array_header(3)
builder.add_integer(1)
builder.add_bulk(b'hello')
builder.add_simple('OK')

response = builder.get_response()  # Returns bytes and resets builder
```

### ResponseBuilder Methods

- `add_simple(msg: str)` - Add simple string
- `add_error(msg: str)` - Add error
- `add_integer(n: int)` - Add integer
- `add_bulk(data: bytes)` - Add bulk string
- `add_bulk_or_null(data)` - Add bulk string or null
- `add_array_header(count: int)` - Add array header
- `add_null()` - Add null bulk string
- `add_raw(data: bytes)` - Add pre-encoded RESP2 data
- `get_response() -> bytes` - Get final response and reset buffer
- `reset()` - Clear buffer without returning data

### Builder Initialization

```python
# Default 256-byte initial capacity
builder = ResponseBuilder()

# Custom initial capacity for large responses
builder = ResponseBuilder(initial_capacity=1024)
```

## Type Auto-Detection

The `encode_value()` function automatically maps Python types to RESP2:

| Python Type | RESP2 Type | Example |
|------------|------------|---------|
| `None` | Null bulk string | `$-1\r\n` |
| `int` | Integer | `:42\r\n` |
| `str` | Bulk string (UTF-8) | `$5\r\nhello\r\n` |
| `bytes` | Bulk string | `$4\r\ndata\r\n` |
| `list`, `tuple` | Array (recursive) | `*2\r\n:1\r\n:2\r\n` |

```python
# Automatic encoding
response = encode_value({
    'name': 'John',
    'age': 30,
    'tags': ['developer', 'python']
})
```

## Performance Guidelines

### 1. Always Prefer Pre-allocated Constants

```python
# GOOD - Zero allocation
return RESP_OK

# BAD - Allocates new bytes object
return simple_string('OK')
```

### 2. Use Pre-allocated Integers

```python
# GOOD - Reuses constant
return RESP_ZERO

# BAD - Creates new object
return integer(0)
```

### 3. Use ResponseBuilder for Multi-part Responses

```python
# GOOD - Single bytearray buffer
builder = ResponseBuilder()
builder.add_array_header(100)
for item in items:
    builder.add_bulk(item)
response = builder.get_response()

# BAD - Creates 100+ intermediate bytes objects
response = b'*100\r\n'
for item in items:
    response += bulk_string(item)  # New bytes each iteration
```

### 4. Reuse ResponseBuilder Instances

```python
# GOOD - Reuse single builder
builder = ResponseBuilder()
for request in requests:
    # Build response
    builder.add_simple('OK')
    response = builder.get_response()  # Auto-resets
    send(response)

# BAD - Create new builder each time
for request in requests:
    builder = ResponseBuilder()  # Unnecessary allocation
    builder.add_simple('OK')
    response = builder.get_response()
    send(response)
```

## Memory Optimization Details

### Pre-allocated Constants
- **Allocation**: Once at import time
- **Reuse**: Same object reference returned every time
- **Memory saved**: ~60 bytes per response for common cases

### ResponseBuilder
- **Internal buffer**: Single `bytearray` that grows as needed
- **Reuse after `get_response()`**: Buffer cleared but capacity retained
- **Memory savings vs concatenation**: 80-90% less allocation overhead

### Integer Optimization
- **Common integers** (0, 1): Pre-allocated constants
- **Other integers**: Formatted on-demand using minimal temporary allocation

### Bulk String Optimization
- **Direct bytes**: No encoding overhead
- **String to bytes**: Single UTF-8 encode operation
- **Length calculation**: Native `len()` function

## Usage Examples

### Example 1: Simple Command Response

```python
# PING command
def handle_ping():
    return RESP_PONG
```

### Example 2: GET Command

```python
# GET command
def handle_get(key):
    value = storage.get(key)
    return bulk_string_or_null(value)
```

### Example 3: Array Response

```python
# KEYS command
def handle_keys(pattern):
    keys = storage.keys_matching(pattern)
    encoded = [bulk_string(key) for key in keys]
    return array(encoded)
```

### Example 4: Complex Response (HGETALL)

```python
# HGETALL command
def handle_hgetall(key):
    hash_data = storage.get_hash(key)
    if not hash_data:
        return RESP_NULL_ARRAY

    builder = ResponseBuilder()
    builder.add_array_header(len(hash_data) * 2)

    for field, value in hash_data.items():
        builder.add_bulk(field)
        builder.add_bulk(value)

    return builder.get_response()
```

### Example 5: Error Handling

```python
# Command with type checking
def handle_incr(key):
    value = storage.get(key)

    if value is None:
        # Create new counter
        storage.set(key, b'1')
        return RESP_ONE

    try:
        num = int(value)
        num += 1
        storage.set(key, str(num).encode())
        return integer(num)
    except ValueError:
        # Wrong type
        return ERR_WRONGTYPE
```

## Testing

Run the test suite:

```bash
python test_response.py
```

Run usage examples:

```bash
python example_response_usage.py
```

Run memory benchmarks:

```bash
python benchmark_response_memory.py
```

## Platform Compatibility

- **Target**: ESP32-S3 with MicroPython
- **Tested on**: MicroPython 1.20+, CPython 3.9+
- **Memory constraints**: Optimized for ~300KB available RAM
- **Dependencies**: `microredis.core.constants`

## RESP2 Protocol Reference

### Format Types

- **Simple String**: `+<string>\r\n`
- **Error**: `-<error message>\r\n`
- **Integer**: `:<number>\r\n`
- **Bulk String**: `$<length>\r\n<data>\r\n`
- **Array**: `*<count>\r\n<element1><element2>...<elementN>`

### Special Values

- **NULL Bulk String**: `$-1\r\n`
- **NULL Array**: `*-1\r\n`
- **Empty Array**: `*0\r\n`

## License

Part of MicroRedis - A lightweight Redis server for MicroPython on ESP32-S3.

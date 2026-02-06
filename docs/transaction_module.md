# MicroRedis Transaction Module

## Overview

The transaction module (`microredis/features/transaction.py`) implements Redis-compatible WATCH/MULTI/EXEC/DISCARD commands with optimistic locking for MicroPython on ESP32-S3.

## Architecture

### Components

1. **TransactionState** - Per-connection state
   - Tracks transaction mode (`in_multi`)
   - Queues commands during MULTI
   - Records watched key versions
   - Maintains error state

2. **TransactionManager** - Global transaction coordinator
   - Manages state for all connections
   - Implements optimistic locking via Storage._version
   - Provides command handlers for WATCH/MULTI/EXEC/DISCARD

### Memory Footprint

```
TransactionManager:    ~48 bytes (2 __slots__)
TransactionState:      ~96 bytes (4 __slots__)
Per watched key:       ~40 bytes (dict entry)
Per queued command:    ~60 bytes (tuple + list)
```

**Optimization**: State is only allocated when client uses WATCH or MULTI.

## Commands

### WATCH key [key ...]

Mark keys for optimistic locking. Records the current version of each key from `Storage._version`. If any watched key is modified before EXEC, the transaction will abort.

**Behavior**:
- Can be called multiple times (accumulates watches)
- Cannot be called inside MULTI (returns error)
- Cleared by EXEC, DISCARD, or UNWATCH
- Version tracking uses monotonic counter in Storage._version

**Returns**: `+OK\r\n` or error

**Example**:
```python
WATCH user:100
WATCH account:200 account:201
```

### UNWATCH

Clear all watched keys for the current connection.

**Behavior**:
- Safe to call even if no keys are watched
- Does not affect transaction mode

**Returns**: `+OK\r\n`

**Example**:
```python
WATCH key1 key2
UNWATCH  # Clears key1 and key2
```

### MULTI

Enter transaction mode. All subsequent commands (except WATCH, MULTI, EXEC, DISCARD) are queued instead of executed.

**Behavior**:
- Cannot be nested (returns error if already in MULTI)
- Clears command queue
- Resets error state

**Returns**: `+OK\r\n` or error if nested

**Example**:
```python
MULTI
SET key1 value1  # Returns QUEUED
SET key2 value2  # Returns QUEUED
```

### EXEC

Execute all queued commands atomically. Checks for WATCH conflicts first.

**Behavior**:
1. Check WATCH conflicts (compare current versions with recorded versions)
2. If conflict detected, return null array and reset state
3. If error occurred during MULTI, return EXECABORT
4. Execute all queued commands in order
5. Return array of results
6. Reset transaction state

**Returns**:
- `*N\r\n<results>` - Array of N command results on success
- `*-1\r\n` - Null array if WATCH conflict detected
- `-EXECABORT ...` - Error if error occurred during MULTI
- `-ERR EXEC without MULTI` - Error if not in MULTI mode

**Example**:
```python
MULTI
SET key value
GET key
EXEC
# Returns: *2\r\n+OK\r\n$5\r\nvalue\r\n
```

### DISCARD

Abort transaction and clear command queue.

**Behavior**:
- Clears command queue
- Exits MULTI mode
- Clears watched keys
- Resets error state

**Returns**: `+OK\r\n` or error if not in MULTI

**Example**:
```python
MULTI
SET key wrong_value
DISCARD  # Transaction aborted, no changes made
```

## Optimistic Locking

### Version Tracking

Every key modification increments `Storage._version[key]`:

```python
def _increment_version(self, key):
    self._version[key] = self._version.get(key, 0) + 1
```

Called by:
- `Storage.set()`
- `Storage.delete()`
- `Storage.rename()`
- Any command that modifies a key

### Conflict Detection

At EXEC time, compare recorded versions with current versions:

```python
for key, old_version in state.watched_keys.items():
    current_version = self._storage._version.get(key, 0)
    if current_version != old_version:
        # Conflict detected - abort transaction
        return RESP_NULL_ARRAY
```

### Lock-Free Design

- No mutexes or locks
- All checks performed at EXEC time
- Failed transactions can be retried
- No deadlocks possible

## Usage Patterns

### Pattern 1: Simple Transaction

```python
MULTI
SET key1 value1
SET key2 value2
INCR counter
EXEC
```

**Use case**: Atomic multi-key updates

### Pattern 2: Optimistic Locking

```python
WATCH balance:alice
WATCH balance:bob

# Read current balances
alice_balance = GET balance:alice
bob_balance = GET balance:bob

# Calculate new balances
new_alice = alice_balance - 100
new_bob = bob_balance + 100

# Execute transfer
MULTI
SET balance:alice new_alice
SET balance:bob new_bob
EXEC  # Aborts if either balance was modified
```

**Use case**: Race-free read-modify-write

### Pattern 3: Retry on Conflict

```python
while True:
    WATCH counter
    current = GET counter
    new_value = current + 1

    MULTI
    SET counter new_value
    result = EXEC

    if result != null_array:
        break  # Success
    # Retry if conflict
```

**Use case**: Lock-free compare-and-swap

## Integration with ConnectionHandler

### Step 1: Initialize TransactionManager

```python
from microredis.features.transaction import TransactionManager

class ConnectionHandler:
    __slots__ = ('storage', 'transaction_mgr', ...)

    def __init__(self, storage):
        self.storage = storage
        self.transaction_mgr = TransactionManager(storage)
```

### Step 2: Add Commands to Dispatch Table

```python
def _build_command_table(self):
    return {
        b'WATCH': (self._cmd_watch, 1, -1),
        b'UNWATCH': (self._cmd_unwatch, 0, 0),
        b'MULTI': (self._cmd_multi, 0, 0),
        b'EXEC': (self._cmd_exec, 0, 0),
        b'DISCARD': (self._cmd_discard, 0, 0),
        # ... other commands ...
    }
```

### Step 3: Modify execute_command

```python
async def execute_command(self, connection, cmd, args):
    # Check if command should be queued
    queued = self.transaction_mgr.queue_command(connection, cmd, args)
    if queued is not None:
        return queued  # RESP_QUEUED or error

    # Special handling for EXEC
    if cmd == b'EXEC':
        return self.transaction_mgr.exec(connection, self)

    # Normal execution
    handler, min_arity, max_arity = self.command_table[cmd]
    return handler(connection, *args)
```

### Step 4: Implement Command Handlers

```python
def _cmd_watch(self, connection, *keys):
    return self.transaction_mgr.watch(connection, *keys)

def _cmd_unwatch(self, connection):
    return self.transaction_mgr.unwatch(connection)

def _cmd_multi(self, connection):
    return self.transaction_mgr.multi(connection)

def _cmd_discard(self, connection):
    return self.transaction_mgr.discard(connection)
```

### Step 5: Cleanup on Disconnect

```python
async def handle_client(self, connection):
    try:
        while connection.is_connected():
            cmd, args = await connection.read_command()
            response = await self.execute_command(connection, cmd, args)
            await connection.write_response(response)
    finally:
        self.transaction_mgr.cleanup_client(connection)
        await connection.close()
```

## Error Handling

### Errors Before EXEC

Commands that fail validation during MULTI return errors immediately and set `error_state`:

```python
MULTI
SET key value  # OK, queued
WATCH key      # Error: forbidden in MULTI, sets error_state
EXEC           # Returns -EXECABORT
```

### Errors During EXEC

Individual command failures during EXEC do not stop execution (Redis behavior):

```python
MULTI
SET key value
INCR string_key  # Will fail but continue
GET key
EXEC
# Returns: *3\r\n+OK\r\n-ERR...\r\n$5\r\nvalue\r\n
```

### WATCH Conflicts

Version mismatch aborts transaction and returns null array:

```python
WATCH counter
# Another client: SET counter 10
MULTI
INCR counter
EXEC  # Returns: *-1\r\n (null array)
```

## Performance Considerations

### Memory Usage

```
Baseline (no transactions):     0 bytes
After WATCH:                    TransactionState (~96 bytes)
Per watched key:                ~40 bytes
Per queued command:             ~60 bytes
```

**Example**: 10 watched keys + 20 queued commands = ~1.6 KB

### Time Complexity

- `WATCH`: O(k) where k = number of keys
- `MULTI`: O(1)
- `queue_command`: O(1)
- `EXEC`: O(k + n) where k = watched keys, n = queued commands
- `DISCARD`: O(1)

### Best Practices

1. **Minimize watched keys** - Only watch keys you actually check
2. **Keep transactions short** - Fewer queued commands = less memory
3. **Cleanup on disconnect** - Always call `cleanup_client()`
4. **Retry on conflict** - Use exponential backoff for high contention
5. **Avoid long transactions** - Release resources quickly

## Testing

Run comprehensive test suite:

```bash
python tests/test_transaction.py
```

Tests cover:
- Basic WATCH/MULTI/EXEC/DISCARD flow
- Optimistic locking and conflict detection
- Error handling (nested MULTI, forbidden commands)
- Multi-client isolation
- Cleanup and state management

See `examples/transaction_example.py` for usage examples.

## Redis Compatibility

### Implemented

- WATCH key [key ...]
- UNWATCH
- MULTI
- EXEC
- DISCARD
- Optimistic locking via version tracking
- QUEUED responses during MULTI
- Null array on conflict
- EXECABORT on errors

### Differences from Redis

1. **Version tracking instead of CAS** - Uses monotonic counter instead of value comparison
2. **Simplified error handling** - Does not track exact command that caused error
3. **No WATCH on non-existent keys warning** - Silently records version 0

### Not Implemented

- WATCH with pub/sub
- Multi-database WATCH (MicroRedis is single-DB)
- WATCH persistence across reconnect

## Future Enhancements

1. **Pipelining optimization** - Batch RESP2 array construction
2. **Timeout support** - Auto-DISCARD after timeout
3. **Statistics** - Track conflict rate, retry count
4. **Debugging** - Log transaction traces
5. **Memory limits** - Max queued commands per transaction

## References

- [Redis Transactions](https://redis.io/docs/manual/transactions/)
- [Optimistic Locking in Redis](https://redis.io/docs/manual/transactions/#optimistic-locking-using-check-and-set)
- Storage._version implementation: `microredis/storage/engine.py`
- Integration example: `examples/transaction_integration.py`

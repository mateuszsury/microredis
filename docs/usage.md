# MicroRedis Usage Guide

Complete guide for running and testing MicroRedis on ESP32-S3 and CPython.

## Quick Start

### On CPython (for testing)

```python
from microredis.main import run
run()
```

Then test with:
```bash
python examples/test_client.py
```

### On ESP32-S3 with MicroPython

```python
from microredis.main import connect_wifi, run

# Connect to WiFi
ip = connect_wifi('YourSSID', 'YourPassword')
print(f'Server at {ip}:6379')

# Start server
run()
```

## Installation

### ESP32-S3 Setup

1. Flash MicroPython to ESP32-S3:
   ```bash
   esptool.py --chip esp32s3 erase_flash
   esptool.py --chip esp32s3 write_flash -z 0x0 esp32s3-micropython.bin
   ```

2. Upload MicroRedis files:
   ```bash
   # Using ampy, rshell, or mpremote
   mpremote cp -r microredis :
   ```

3. Create `main.py` on ESP32:
   ```python
   from microredis.main import connect_wifi, run

   connect_wifi('SSID', 'password')
   run()
   ```

## Server Configuration

### Basic Server

```python
from microredis.main import MicroRedisServer
import asyncio

server = MicroRedisServer('0.0.0.0', 6379)
asyncio.run(server.start())
```

### Custom Configuration

```python
from microredis.main import MicroRedisServer

# Bind to specific IP
server = MicroRedisServer('192.168.1.100', 16379)

# Get server info
info = server.get_info()
print(info)
# {'host': '192.168.1.100', 'port': 16379, 'running': False,
#  'clients': 0, 'max_clients': 8, 'keys': 0}
```

## Supported Commands (Phase 1)

### Connection Commands

- `PING [message]` - Test connection, returns PONG or message
- `ECHO message` - Echo the message
- `QUIT` - Close connection

### Server Commands

- `COMMAND` - Get command metadata
- `INFO [section]` - Server information (server, memory, stats, keyspace)

### Key-Value Commands

- `GET key` - Get value of key
- `SET key value [EX seconds] [PX milliseconds] [NX|XX]` - Set key
  - `EX seconds` - Set expiry in seconds
  - `PX milliseconds` - Set expiry in milliseconds
  - `NX` - Only set if key doesn't exist
  - `XX` - Only set if key exists
- `DEL key [key ...]` - Delete keys, returns count
- `EXISTS key [key ...]` - Check if keys exist, returns count

## Testing

### Using redis-cli

```bash
# Connect to MicroRedis
redis-cli -h 127.0.0.1 -p 6379

# Test commands
127.0.0.1:6379> PING
PONG

127.0.0.1:6379> SET mykey myvalue
OK

127.0.0.1:6379> GET mykey
"myvalue"

127.0.0.1:6379> SET tempkey tempvalue EX 10
OK

127.0.0.1:6379> INFO memory
# Memory
used_memory:245760
used_memory_human:240.0K
```

### Using Python Client

```python
from examples.test_client import RedisClient

client = RedisClient('127.0.0.1', 6379)

# Send commands
response = client.send_command('SET', 'key', 'value')
print(response)  # b'+OK\r\n'

response = client.send_command('GET', 'key')
print(response)  # b'$5\r\nvalue\r\n'

client.close()
```

## Memory Management

MicroRedis automatically manages memory on ESP32-S3:

### Automatic GC Configuration

```python
gc.threshold(32768)  # 32KB threshold
gc.collect()         # Initial collection
```

### Memory Monitoring

Server automatically monitors memory every 60 seconds:
- Logs free and allocated memory
- Forces `gc.collect()` if free memory < 50KB
- Triggers GC after each client disconnect

### Manual Memory Check

```python
import gc

# Check memory
print(f'Free: {gc.mem_free()} bytes')
print(f'Allocated: {gc.mem_alloc()} bytes')

# Force collection
gc.collect()
```

## Performance Characteristics

### Memory Usage (ESP32-S3)

- Server overhead: ~40KB
- Per-client overhead: ~8KB (4KB buffer + connection state)
- Per-key overhead: ~100-200 bytes (depending on key/value size)
- Maximum clients: 8 (configurable in `constants.py`)

### Throughput

- Single client: ~1000-2000 commands/sec
- Multiple clients: ~500-1000 commands/sec per client
- Network latency: WiFi adds ~10-50ms

### Limitations

- Maximum buffer size: 4KB per connection
- Maximum command size: 4KB
- Maximum value size: Limited by available RAM
- No persistence (in-memory only)

## WiFi Connection (ESP32-S3)

### Basic Connection

```python
from microredis.main import connect_wifi

ip = connect_wifi('SSID', 'password')
print(f'IP: {ip}')
```

### With Timeout

```python
try:
    ip = connect_wifi('SSID', 'password', timeout=60)
except RuntimeError as e:
    print(f'WiFi failed: {e}')
```

### Network Configuration

```python
import network

wlan = network.WLAN(network.STA_IF)
wlan.active(True)

# Static IP
wlan.ifconfig(('192.168.1.100', '255.255.255.0', '192.168.1.1', '8.8.8.8'))

# Connect
wlan.connect('SSID', 'password')
```

## Logging

MicroRedis uses print-based logging (no logging module overhead):

```
[MicroRedis] Initialized on 0.0.0.0:6379
[MicroRedis] Starting on 0.0.0.0:6379
[MicroRedis] Server started on 0.0.0.0:6379
[MicroRedis] Memory: 245KB free
[MicroRedis] Client connected: ('192.168.1.50', 54321) (1/8)
[MicroRedis] Client disconnected: ('192.168.1.50', 54321) (0/8)
[MicroRedis] Memory check: 230KB free, 270KB allocated
```

## Error Handling

### Connection Errors

```python
try:
    from microredis.main import MicroRedisServer
    server = MicroRedisServer('0.0.0.0', 6379)
    await server.start()
except OSError as e:
    print(f'Failed to bind: {e}')
    # Port already in use or permission denied
```

### Client Errors

Server handles client errors gracefully:
- Timeout after 30s of inactivity
- Automatic cleanup on disconnect
- Error responses for invalid commands

### Memory Errors

```python
import gc

# Monitor memory
gc.threshold(32768)

# In low memory situations
if gc.mem_free() < 50000:
    gc.collect()
```

## Advanced Usage

### Programmatic Control

```python
import asyncio
from microredis.main import MicroRedisServer

async def main():
    server = MicroRedisServer('0.0.0.0', 6379)

    # Start in background
    task = asyncio.create_task(server.start())

    # Do other work
    await asyncio.sleep(60)

    # Stop server
    await server.stop()

    # Wait for task
    await task

asyncio.run(main())
```

### Custom Commands

```python
from microredis.network.connection import ConnectionHandler
from microredis.core.response import simple_string

# Add custom command
async def cmd_hello(conn, args):
    return simple_string('Hello from MicroRedis!')

handler = ConnectionHandler(storage)
handler.register_command('HELLO', cmd_hello, 0, 0)
```

## Troubleshooting

### Server won't start

- Check if port is already in use: `netstat -an | grep 6379`
- Try different port: `run(port=16379)`
- Check WiFi connection on ESP32

### Out of memory

- Reduce MAX_CLIENTS in `constants.py`
- Reduce BUFFER_SIZE in `constants.py`
- Monitor with `gc.mem_free()`

### Slow performance

- Check WiFi signal strength on ESP32
- Reduce client count
- Use smaller keys/values
- Enable GC threshold tuning

### Client timeout

- Increase READ_TIMEOUT_MS in `connection.py`
- Check network latency
- Reduce command size

## Next Steps

Phase 2 will add:
- Additional data types (Hash, List, Set, Sorted Set)
- Transactions (MULTI/EXEC/WATCH)
- Pub/Sub
- More string commands
- Persistence (optional)

"""
MicroRedis Advanced Commands Module

This module contains advanced Redis command implementations optimized for
MicroPython on ESP32-S3 with memory constraints (~300KB available RAM).

Advanced Command Categories:
- Bitmaps: Bit-level operations on strings (SETBIT, GETBIT, BITCOUNT, BITOP, BITPOS)
- HyperLogLog: Probabilistic cardinality estimation for unique counts
- Streams: Append-only log data structure for time-series and event sourcing

These commands enable specialized use cases:
- Bitmaps: Space-efficient boolean flags, user activity tracking, bloom filters
- HyperLogLog: Memory-efficient unique visitor counting, set cardinality estimation
- Streams: IoT sensor data logging, event streams, message queues

Memory Considerations:
- Bitmaps: Very memory-efficient (8 bits per byte), directly use string storage
- HyperLogLog: Fixed ~12KB memory per key (standard precision=14)
- Streams: Memory grows with entries, configure MAXLEN for memory-constrained devices
- On ESP32-S3, monitor memory usage when using Streams with high throughput

Platform Compatibility:
- Primary: MicroPython on ESP32-S3
- Secondary: CPython 3.9+ for testing

Implementation Status:
- Bitmaps: Basic operations (SETBIT, GETBIT, BITCOUNT) implemented via StringOperations
- HyperLogLog: Planned for Phase 3+
- Streams: IMPLEMENTED (XADD, XLEN, XRANGE, XREVRANGE, XREAD, XTRIM)

Usage:
    from microredis.commands import StreamOperations

    # Bitmap operations (already available via StringOperations)
    from microredis.storage.datatypes import StringOperations
    result = StringOperations.setbit(storage, b'bitmap:users', 42, 1)
    count = StringOperations.bitcount(storage, b'bitmap:users')

    # HyperLogLog (future)
    # hll = HyperLogLogOperations()
    # hll.pfadd(storage, b'unique:visitors', [b'user1', b'user2'])
    # cardinality = hll.pfcount(storage, b'unique:visitors')

    # Streams (available now)
    entry_id = StreamOperations.xadd(storage, b'sensor:temp', b'*', {b'value': b'23.5', b'unit': b'C'})
    length = StreamOperations.xlen(storage, b'sensor:temp')
    entries = StreamOperations.xrange(storage, b'sensor:temp', b'-', b'+', count=10)
    entries = StreamOperations.xread(storage, {b'sensor:temp': b'0-0'}, count=10)
"""

# Current imports:
from microredis.commands.streams import StreamOperations

# Future imports when classes are implemented:
# from .bitmap import BitmapOperations
# from .hyperloglog import HyperLogLogOperations

# Note: Basic bitmap operations (SETBIT, GETBIT, BITCOUNT) are already
# implemented in microredis.storage.datatypes.string.StringOperations
# This module will provide additional bitmap operations like BITOP and BITPOS

__all__ = [
    'StreamOperations',
    # Will export when implemented:
    # 'BitmapOperations',
    # 'HyperLogLogOperations',
]

# Version info
__version__ = '0.1.0'
__author__ = 'MicroRedis Team'

# Platform compatibility
try:
    import uasyncio as asyncio
except ImportError:
    import asyncio

# HyperLogLog constants (for future implementation)
try:
    from micropython import const
    HLL_PRECISION = const(14)  # Standard Redis precision
    HLL_REGISTERS = const(16384)  # 2^14 registers
    HLL_SIZE = const(12288)  # ~12KB per HLL key
except ImportError:
    # CPython fallback
    HLL_PRECISION = 14
    HLL_REGISTERS = 16384
    HLL_SIZE = 12288

# Stream constants (for future implementation)
try:
    from micropython import const
    STREAM_DEFAULT_MAXLEN = const(1000)  # Default max entries for memory safety
    STREAM_ID_AUTO = const(0)  # Auto-generate entry ID
except ImportError:
    # CPython fallback
    STREAM_DEFAULT_MAXLEN = 1000
    STREAM_ID_AUTO = 0

"""
MicroRedis Data Types Module

This module provides Redis-compatible data type implementations optimized for
MicroPython on ESP32-S3 with memory constraints (~300KB available RAM).

Supported Data Types:
- String: Simple key-value storage (bytes)
- List: Doubly-linked list for LPUSH/RPUSH/LPOP/RPOP operations
- Hash: Dictionary-like field-value mappings
- Set: Unique string collection with set operations
- Sorted Set: Ordered collection with score-based ranking

Memory Optimization Strategies:
1. __slots__ used in all classes to minimize instance overhead
2. Native Python collections (dict, list) where appropriate
3. Lazy allocation - data structures created only when needed
4. Pre-allocated buffers for encoding/decoding operations
5. Zero-copy operations with memoryview where possible

Each data type class provides:
- Type-specific operations (e.g., LPUSH for lists, HSET for hashes)
- Serialization for persistence (future feature)
- Memory-efficient internal representation
- RESP2 encoding helpers

Platform Compatibility:
- Primary: MicroPython on ESP32-S3
- Secondary: CPython 3.9+ for testing

Note: This module is currently a placeholder. Data type classes will be
implemented as needed for Phase 2+ command support.
"""

# Import data type operation classes
from .string import StringOperations
from .hash import HashType
from .list import ListOps as ListOperations
from .set import SetOperations
from .zset import SortedSetOperations

__all__ = [
    'StringOperations',
    'HashType',
    'ListOperations',
    'SetOperations',
    'SortedSetOperations',
]

# Version info
__version__ = '0.1.0'
__author__ = 'MicroRedis Team'

# Import type constants from canonical source (storage/engine.py)
from microredis.storage.engine import (
    TYPE_STRING, TYPE_HASH, TYPE_LIST, TYPE_SET, TYPE_ZSET as TYPE_SORTED_SET,
    TYPE_STREAM, TYPE_NAMES
)

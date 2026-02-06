"""
MicroRedis Constants Module

Defines all protocol constants, network parameters, and storage limits
for the MicroRedis server implementation on ESP32-S3.

Platform: ESP32-S3 with MicroPython
Memory-optimized using frozen constants via micropython.const()
"""

# Import const() with fallback for PC-based testing
try:
    from micropython import const
except ImportError:
    # Fallback for testing on CPython - const() acts as identity function
    const = lambda x: x

# =============================================================================
# RESP2 Protocol Markers
# =============================================================================
# Redis Serialization Protocol (RESP2) type indicators
# Using ord() to get byte values for protocol parsing

SIMPLE_STRING = const(ord('+'))  # Simple string response: +OK\r\n
ERROR = const(ord('-'))          # Error response: -ERR message\r\n
INTEGER = const(ord(':'))        # Integer response: :1000\r\n
BULK_STRING = const(ord('$'))    # Bulk string: $6\r\nfoobar\r\n
ARRAY = const(ord('*'))          # Array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n

# =============================================================================
# Network Configuration
# =============================================================================
# Optimized for ESP32-S3 with limited RAM (~512KB total, ~300KB available)

DEFAULT_PORT = const(6379)       # Standard Redis port
MAX_CLIENTS = const(8)           # Maximum concurrent client connections
                                 # Limited to 8 to preserve memory on ESP32-S3

BUFFER_SIZE = const(4096)        # Socket read/write buffer size (4KB)
                                 # Trade-off between memory usage and throughput
                                 # Allows handling of moderately large commands

# =============================================================================
# Storage Optimization Constants
# =============================================================================
# Compact data structure thresholds for memory-efficient storage

ZIPLIST_MAX_ENTRIES = const(64)  # Maximum entries before converting ziplist
                                 # to hash table. Ziplist uses ~40% less RAM
                                 # for small collections

INTSET_MAX_ENTRIES = const(512)  # Maximum integer set size before converting
                                 # to regular set. Intset is highly compressed
                                 # for sets containing only integers

# =============================================================================
# TTL and Expiry Management
# =============================================================================
# Active expiry checking parameters for efficient key eviction

EXPIRY_CHECK_INTERVAL_MS = const(100)  # Check expired keys every 100ms
                                       # Balance between CPU usage and
                                       # expiry accuracy

EXPIRY_SAMPLE_SIZE = const(20)        # Sample 20 random keys per check cycle
                                      # Probabilistic expiry algorithm to
                                      # avoid scanning entire keyspace

# =============================================================================
# Protocol Limits (DoS prevention)
# =============================================================================
# Limits to prevent denial-of-service attacks via malformed requests

MAX_ARRAY_DEPTH = const(32)          # Maximum nesting depth for arrays
                                     # Prevents stack overflow on deeply nested

MAX_BULK_SIZE = const(64 * 1024)     # Maximum bulk string size (64KB)
                                     # Limits memory consumption per request
                                     # Reduced for ESP32-S3 with ~300KB RAM

MAX_ARRAY_SIZE = const(8192)         # Maximum array elements (8K)
                                     # Prevents memory exhaustion from *99999999

MAX_RESPONSE_SIZE = const(128 * 1024)  # Maximum response size (128KB)
                                       # Limits memory for response building

MAX_KEYS = const(50000)                 # Maximum number of keys in storage
                                        # Prevents memory exhaustion from
                                        # malicious clients creating many keys

# =============================================================================
# Protocol Line Terminators
# =============================================================================
# RESP protocol requires CRLF (\r\n) line endings

CRLF = b'\r\n'  # Carriage Return + Line Feed (as bytes for binary protocol)

"""
MicroRedis Storage Engine Module

Dict-based storage engine for MicroRedis on ESP32-S3 with MicroPython.
Implements Redis-like storage with types, TTL, and pattern matching.

Platform: ESP32-S3 with MicroPython
RAM Constraints: ~300KB available for application

Memory Optimizations:
- __slots__ for Storage class to minimize instance size
- Lazy expiry checking (only on access)
- bytes keys instead of strings
- Minimal temporary allocations
- Separate dictionaries for data/types/expires to reduce overhead per key
"""

import time
import sys
from microredis.core.constants import const, MAX_KEYS

# CPython compatibility: MicroPython ticks_* functions
if not hasattr(time, 'ticks_ms'):
    time.ticks_ms = lambda: int(time.time() * 1000) & 0x3FFFFFFF
if not hasattr(time, 'ticks_diff'):
    time.ticks_diff = lambda a, b: (a - b) & 0x3FFFFFFF if (a - b) & 0x3FFFFFFF < 0x20000000 else (a - b) | ~0x3FFFFFFF
if not hasattr(time, 'ticks_add'):
    time.ticks_add = lambda a, b: (a + b) & 0x3FFFFFFF

# Import const() with fallback for testing
try:
    from micropython import const
except ImportError:
    const = lambda x: x

# MicroPython epoch offset: MicroPython time.time() returns seconds since 2000-01-01
# Unix epoch is 1970-01-01. Difference = 946684800 seconds.
# On CPython, time.time() already returns Unix epoch, so offset = 0.
_MICROPYTHON_EPOCH_OFFSET = 946684800 if sys.platform in ('esp32', 'rp2', 'pyboard') else 0

# Storage type constants
TYPE_STRING = const(0)
TYPE_HASH = const(1)
TYPE_LIST = const(2)
TYPE_SET = const(3)
TYPE_ZSET = const(4)
TYPE_STREAM = const(5)

# Type name mappings (pre-allocated bytes)
TYPE_NAMES = {
    TYPE_STRING: b'string',
    TYPE_HASH: b'hash',
    TYPE_LIST: b'list',
    TYPE_SET: b'set',
    TYPE_ZSET: b'zset',
    TYPE_STREAM: b'stream',
}

# TTL return values
TTL_NOT_EXISTS = const(-2)
TTL_NO_EXPIRY = const(-1)


class Storage:
    """
    Main storage engine for MicroRedis.

    Manages key-value storage with type checking, TTL, and WATCH versioning.
    Uses separate dictionaries to minimize per-key overhead.

    Memory Layout:
    - _data: {bytes: value} - actual data storage
    - _types: {bytes: int} - type markers (only if not TYPE_STRING)
    - _expires: {bytes: int} - TTL timestamps in milliseconds
    - _version: {bytes: int} - version counters for WATCH/MULTI/EXEC
    """

    __slots__ = ('_data', '_types', '_expires', '_version', '_expiry_manager', '_last_access')

    def __init__(self):
        """Initialize empty storage engine."""
        self._data = {}      # Main data storage: key -> value
        self._types = {}     # Type tracking: key -> TYPE_* (default: TYPE_STRING)
        self._expires = {}   # TTL tracking: key -> timestamp_ms
        self._version = {}   # Version tracking: key -> int (for WATCH)
        self._expiry_manager = None  # ExpiryManager instance (set via set_expiry_manager)
        self._last_access = {}  # Last access time: key -> ticks_ms (for LRU)

    # =========================================================================
    # Internal Helper Methods
    # =========================================================================

    @staticmethod
    def _get_timestamp_ms():
        """
        Get current timestamp in milliseconds.

        Uses time.ticks_ms() which is monotonic and wraps at ~25 days.
        For TTL calculations, this is sufficient as Redis TTLs are typically
        much shorter than 25 days.

        Returns:
            int: Current time in milliseconds
        """
        return time.ticks_ms()

    def _is_expired(self, key):
        """
        Check if a key is expired without deleting it.

        Args:
            key: bytes - key to check

        Returns:
            bool: True if expired, False otherwise
        """
        if key not in self._expires:
            return False

        # Handle ticks_ms() wraparound using ticks_diff
        # Returns positive if deadline is in future, negative if past
        remaining = time.ticks_diff(self._expires[key], time.ticks_ms())
        return remaining <= 0

    def _delete_if_expired(self, key):
        """
        Delete key if it's expired (lazy expiry).

        Args:
            key: bytes - key to check and potentially delete

        Returns:
            bool: True if key was expired and deleted, False otherwise
        """
        if self._is_expired(key):
            # Clean up all associated data
            del self._data[key]
            del self._expires[key]
            self._types.pop(key, None)  # May not exist for TYPE_STRING
            self._version.pop(key, None)  # May not exist
            return True
        return False

    def _check_type(self, key, expected_type):
        """
        Check if key exists and has the expected type.

        Args:
            key: bytes - key to check
            expected_type: int - TYPE_* constant

        Returns:
            bool: True if key exists and has correct type, False otherwise
        """
        if key not in self._data:
            return False

        # Check expiry first
        if self._delete_if_expired(key):
            return False

        # Get type (default to TYPE_STRING if not in _types dict)
        actual_type = self._types.get(key, TYPE_STRING)
        return actual_type == expected_type

    def _increment_version(self, key):
        """
        Increment version counter for a key (used by WATCH).

        Args:
            key: bytes - key to increment version for
        """
        self._version[key] = self._version.get(key, 0) + 1

    def _set_expiry_ms(self, key, timestamp_ms):
        """
        Set expiry timestamp for a key in milliseconds.

        Args:
            key: bytes - key to set expiry for
            timestamp_ms: int - absolute timestamp in milliseconds
        """
        self._expires[key] = timestamp_ms
        if self._expiry_manager:
            self._expiry_manager.add_expiry(key, timestamp_ms)

    def _set_expiry_relative_ms(self, key, milliseconds):
        """
        Set expiry relative to current time in milliseconds.

        Args:
            key: bytes - key to set expiry for
            milliseconds: int - milliseconds from now
        """
        # Use ticks_add to handle wraparound correctly
        deadline = time.ticks_add(time.ticks_ms(), milliseconds)
        self._expires[key] = deadline

    # =========================================================================
    # Pattern Matching for KEYS command
    # =========================================================================

    @staticmethod
    def _match_pattern(key, pattern):
        """
        Simple glob pattern matching for KEYS command.

        Supports:
        - * : match any sequence of characters
        - ? : match single character
        - [abc] : match one character from set
        - [a-z] : match one character from range
        - \\ : escape special characters

        Args:
            key: bytes - key to match
            pattern: bytes - glob pattern

        Returns:
            bool: True if key matches pattern
        """
        # Fast path: exact match
        if pattern == key:
            return True

        # Fast path: pattern is just '*'
        if pattern == b'*':
            return True

        # Full glob matching
        pi = 0  # pattern index
        ki = 0  # key index
        plen = len(pattern)
        klen = len(key)

        # Stack for backtracking on * wildcards
        star_pi = -1  # position after last *
        star_ki = -1  # position in key when * was encountered

        while ki < klen:
            if pi < plen:
                pc = pattern[pi]

                if pc == ord(b'*'):
                    # Wildcard - save position for backtracking
                    star_pi = pi + 1
                    star_ki = ki
                    pi += 1
                    continue

                elif pc == ord(b'?'):
                    # Single character wildcard
                    pi += 1
                    ki += 1
                    continue

                elif pc == ord(b'['):
                    # Character class
                    pi += 1
                    if pi >= plen:
                        return False

                    # Check for negation
                    negate = pattern[pi] == ord(b'^')
                    if negate:
                        pi += 1

                    matched = False
                    while pi < plen and pattern[pi] != ord(b']'):
                        if pi + 2 < plen and pattern[pi + 1] == ord(b'-'):
                            # Range: [a-z]
                            if pattern[pi] <= key[ki] <= pattern[pi + 2]:
                                matched = True
                            pi += 3
                        else:
                            # Single char
                            if pattern[pi] == key[ki]:
                                matched = True
                            pi += 1

                    if pi >= plen:  # Unclosed bracket
                        return False

                    pi += 1  # Skip closing ]

                    if matched == negate:  # XOR logic
                        # No match, try backtracking to last *
                        if star_pi != -1:
                            pi = star_pi
                            star_ki += 1
                            ki = star_ki
                            continue
                        return False

                    ki += 1
                    continue

                elif pc == ord(b'\\'):
                    # Escape character
                    pi += 1
                    if pi >= plen:
                        return False
                    pc = pattern[pi]

                # Literal character match
                if key[ki] == pc:
                    pi += 1
                    ki += 1
                else:
                    # No match, try backtracking to last *
                    if star_pi != -1:
                        pi = star_pi
                        star_ki += 1
                        ki = star_ki
                    else:
                        return False
            else:
                # Pattern exhausted but key remains
                if star_pi != -1:
                    pi = star_pi
                    star_ki += 1
                    ki = star_ki
                else:
                    return False

        # Skip any trailing * in pattern
        while pi < plen and pattern[pi] == ord(b'*'):
            pi += 1

        # Match if both exhausted
        return pi == plen

    # =========================================================================
    # Basic Operations (Faza 1)
    # =========================================================================

    def get(self, key):
        """
        Get value of a string key.

        Args:
            key: bytes - key to retrieve

        Returns:
            bytes | None: value if exists and is string type, None otherwise
        """
        if key not in self._data:
            return None

        # Check and clean expired keys
        if self._delete_if_expired(key):
            return None

        # Type check (must be string)
        if not self._check_type(key, TYPE_STRING):
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

        self._last_access[key] = time.ticks_ms()
        return self._data[key]

    def set(self, key, value, ex=None, px=None, nx=False, xx=False):
        """
        Set string value with optional TTL and conditions.

        Args:
            key: bytes - key to set
            value: bytes - value to store
            ex: int | None - expire time in seconds
            px: int | None - expire time in milliseconds
            nx: bool - only set if key does NOT exist
            xx: bool - only set if key DOES exist

        Returns:
            bool: True if value was set, False otherwise

        Raises:
            MemoryError: If MAX_KEYS limit would be exceeded
        """
        # Check expiry first
        self._delete_if_expired(key)

        exists = key in self._data

        # NX: only set if NOT exists
        if nx and exists:
            return False

        # XX: only set if exists
        if xx and not exists:
            return False

        # Check MAX_KEYS limit before adding new key
        if not exists and len(self._data) >= MAX_KEYS:
            raise MemoryError("OOM command not allowed: max keys limit reached")

        # Store value and mark as string type
        self._data[key] = value
        self._types.pop(key, None)  # Remove type marker (default to TYPE_STRING)

        # Set expiry if specified
        if px is not None:
            self._set_expiry_relative_ms(key, px)
        elif ex is not None:
            self._set_expiry_relative_ms(key, ex * 1000)
        else:
            # Remove any existing expiry
            self._expires.pop(key, None)

        # Increment version for WATCH
        self._increment_version(key)

        return True

    def delete(self, *keys):
        """
        Delete one or more keys.

        Args:
            *keys: bytes - keys to delete

        Returns:
            int: number of keys that were deleted
        """
        count = 0
        for key in keys:
            if key in self._data:
                del self._data[key]
                self._types.pop(key, None)
                self._expires.pop(key, None)
                self._increment_version(key)  # Keep version so WATCH detects delete+recreate
                count += 1

        return count

    def exists(self, *keys):
        """
        Check how many keys exist.

        Args:
            *keys: bytes - keys to check

        Returns:
            int: number of keys that exist
        """
        count = 0
        for key in keys:
            if key in self._data:
                # Check expiry
                if not self._delete_if_expired(key):
                    count += 1

        return count

    def type(self, key):
        """
        Get type of key.

        Args:
            key: bytes - key to check

        Returns:
            bytes: type name (string, hash, list, set, zset, none)
        """
        if key not in self._data:
            return b'none'

        # Check expiry
        if self._delete_if_expired(key):
            return b'none'

        # Get type (default to TYPE_STRING)
        type_id = self._types.get(key, TYPE_STRING)
        return TYPE_NAMES.get(type_id, b'none')

    # =========================================================================
    # TTL Operations
    # =========================================================================

    def expire(self, key, seconds):
        """
        Set key expiry in seconds.

        Args:
            key: bytes - key to set expiry for
            seconds: int - seconds until expiry

        Returns:
            bool: True if expiry was set, False if key doesn't exist
        """
        if key not in self._data:
            return False

        # Check expiry
        if self._delete_if_expired(key):
            return False

        self._set_expiry_relative_ms(key, seconds * 1000)
        return True

    def expireat(self, key, timestamp):
        """
        Set key expiry at Unix timestamp (seconds).

        Args:
            key: bytes - key to set expiry for
            timestamp: int - Unix timestamp in seconds

        Returns:
            bool: True if expiry was set, False if key doesn't exist
        """
        if key not in self._data:
            return False

        # Check expiry
        if self._delete_if_expired(key):
            return False

        # Convert Unix timestamp to relative milliseconds from now
        current_unix = time.time() + _MICROPYTHON_EPOCH_OFFSET
        offset_ms = int((timestamp - current_unix) * 1000)
        if offset_ms <= 0:
            # Already expired - delete the key
            self.delete(key)
            return True
        deadline = time.ticks_add(time.ticks_ms(), offset_ms)

        self._set_expiry_ms(key, deadline)
        return True

    def pexpire(self, key, milliseconds):
        """
        Set key expiry in milliseconds.

        Args:
            key: bytes - key to set expiry for
            milliseconds: int - milliseconds until expiry

        Returns:
            bool: True if expiry was set, False if key doesn't exist
        """
        if key not in self._data:
            return False

        # Check expiry
        if self._delete_if_expired(key):
            return False

        self._set_expiry_relative_ms(key, milliseconds)
        return True

    def pexpireat(self, key, ms_timestamp):
        """
        Set key expiry at Unix timestamp in milliseconds.

        Args:
            key: bytes - key to set expiry for
            ms_timestamp: int - Unix timestamp in milliseconds

        Returns:
            bool: True if expiry was set, False if key doesn't exist
        """
        if key not in self._data:
            return False

        # Check expiry
        if self._delete_if_expired(key):
            return False

        # Convert Unix ms timestamp to relative milliseconds from now
        current_unix_ms = int((time.time() + _MICROPYTHON_EPOCH_OFFSET) * 1000)
        offset_ms = ms_timestamp - current_unix_ms
        if offset_ms <= 0:
            self.delete(key)
            return True
        deadline = time.ticks_add(time.ticks_ms(), offset_ms)

        self._set_expiry_ms(key, deadline)
        return True

    def ttl(self, key):
        """
        Get time to live in seconds.

        Args:
            key: bytes - key to check

        Returns:
            int: -2 if key doesn't exist, -1 if no expiry, >0 for TTL in seconds
        """
        if key not in self._data:
            return TTL_NOT_EXISTS

        # Check expiry
        if self._delete_if_expired(key):
            return TTL_NOT_EXISTS

        if key not in self._expires:
            return TTL_NO_EXPIRY

        # Calculate remaining time
        remaining_ms = time.ticks_diff(self._expires[key], time.ticks_ms())

        if remaining_ms <= 0:
            # Expired but not yet deleted
            self._delete_if_expired(key)
            return TTL_NOT_EXISTS

        # Convert to seconds (floor, same as Redis)
        return remaining_ms // 1000

    def pttl(self, key):
        """
        Get time to live in milliseconds.

        Args:
            key: bytes - key to check

        Returns:
            int: -2 if key doesn't exist, -1 if no expiry, >0 for TTL in milliseconds
        """
        if key not in self._data:
            return TTL_NOT_EXISTS

        # Check expiry
        if self._delete_if_expired(key):
            return TTL_NOT_EXISTS

        if key not in self._expires:
            return TTL_NO_EXPIRY

        # Calculate remaining time
        remaining_ms = time.ticks_diff(self._expires[key], time.ticks_ms())

        if remaining_ms <= 0:
            # Expired but not yet deleted
            self._delete_if_expired(key)
            return TTL_NOT_EXISTS

        return remaining_ms

    def persist(self, key):
        """
        Remove TTL from a key.

        Args:
            key: bytes - key to persist

        Returns:
            bool: True if TTL was removed, False if key doesn't exist or has no TTL
        """
        if key not in self._data:
            return False

        # Check expiry
        if self._delete_if_expired(key):
            return False

        if key not in self._expires:
            return False

        del self._expires[key]
        return True

    # =========================================================================
    # Key Operations
    # =========================================================================

    def keys(self, pattern):
        """
        Find all keys matching a pattern.

        Args:
            pattern: bytes - glob pattern (* ? [abc])

        Returns:
            list[bytes]: list of matching keys
        """
        result = []

        # Check all keys
        for key in list(self._data.keys()):  # Copy to avoid modification during iteration
            # Skip expired keys
            if self._delete_if_expired(key):
                continue

            # Match pattern
            if self._match_pattern(key, pattern):
                result.append(key)

        return result

    def rename(self, key, newkey):
        """
        Rename a key.

        Args:
            key: bytes - source key
            newkey: bytes - destination key

        Returns:
            bool: True if renamed

        Raises:
            KeyError: If source key doesn't exist
        """
        if key not in self._data:
            raise KeyError("no such key")

        # Check expiry
        if self._delete_if_expired(key):
            raise KeyError("no such key")

        # Delete destination if exists
        if newkey in self._data:
            self.delete(newkey)

        # Move data
        self._data[newkey] = self._data[key]

        # Move type if exists
        if key in self._types:
            self._types[newkey] = self._types[key]
            del self._types[key]

        # Move expiry if exists
        if key in self._expires:
            self._expires[newkey] = self._expires[key]
            del self._expires[key]

        # Move version if exists
        if key in self._version:
            self._version[newkey] = self._version[key]
            del self._version[key]

        # Delete old key
        del self._data[key]

        # Increment version only for newkey (source key is deleted, don't create new entry)
        self._increment_version(newkey)

        return True

    def renamenx(self, key, newkey):
        """
        Rename a key only if new key doesn't exist.

        Args:
            key: bytes - source key
            newkey: bytes - destination key

        Returns:
            int: 1 if renamed, 0 if dest already exists

        Raises:
            KeyError: If source key doesn't exist
        """
        if key not in self._data:
            raise KeyError("no such key")

        # Check expiry on source
        if self._delete_if_expired(key):
            raise KeyError("no such key")

        # Check if destination exists (and clean if expired)
        self._delete_if_expired(newkey)
        if newkey in self._data:
            return 0

        # Move data (same as rename but without deleting destination)
        self._data[newkey] = self._data[key]

        # Move type if exists
        if key in self._types:
            self._types[newkey] = self._types[key]
            del self._types[key]

        # Move expiry if exists
        if key in self._expires:
            self._expires[newkey] = self._expires[key]
            del self._expires[key]

        # Move version if exists
        if key in self._version:
            self._version[newkey] = self._version[key]
            del self._version[key]

        # Delete old key
        del self._data[key]

        # Increment version only for newkey (source key is deleted, don't create new entry)
        self._increment_version(newkey)

        return 1

    # =========================================================================
    # TTL-preserving and utility methods
    # =========================================================================

    def set_value_only(self, key, value):
        """
        Set value without clearing TTL. Used by APPEND, INCR, etc.

        Args:
            key: bytes - key to update
            value: bytes - new value
        """
        self._data[key] = value
        self._increment_version(key)

    def set_expiry_manager(self, manager):
        """
        Set the ExpiryManager instance for active expiry integration.

        Args:
            manager: ExpiryManager instance
        """
        self._expiry_manager = manager

    def check_can_create_key(self, key):
        """
        Check if a new key can be created (MAX_KEYS limit).

        Args:
            key: bytes - key to check

        Raises:
            MemoryError: If MAX_KEYS limit would be exceeded
        """
        if key not in self._data and len(self._data) >= MAX_KEYS:
            raise MemoryError("OOM command not allowed: max keys limit reached")

    def flush(self):
        """
        Clear all data, types, expires, versions, and ExpiryManager heap.
        Used by FLUSHDB/FLUSHALL.
        """
        self._data.clear()
        self._types.clear()
        self._expires.clear()
        self._version.clear()
        self._last_access.clear()
        if self._expiry_manager:
            self._expiry_manager.clear()

"""
MicroRedis String Operations Module

String data type operations for MicroRedis on ESP32-S3 with MicroPython.
Implements Redis string commands: APPEND, STRLEN, GETRANGE, SETRANGE,
INCR, INCRBY, INCRBYFLOAT, DECR, DECRBY, SETBIT, GETBIT, BITCOUNT,
MGET, MSET, MSETNX.

Platform: ESP32-S3 with MicroPython
RAM Constraints: ~300KB available for application

Memory Optimizations:
- All methods are static/class methods (no instance data)
- Direct integration with storage engine (no data copying)
- Efficient byte manipulation for bitwise operations
- Minimal temporary allocations
"""

from microredis.storage.engine import TYPE_STRING

# Import const() with fallback for testing
try:
    from micropython import const
except ImportError:
    const = lambda x: x


class StringOperations:
    """
    String data type operations for MicroRedis.

    All methods are static and operate directly on the storage engine.
    Designed for minimal memory overhead on ESP32-S3.
    """

    __slots__ = ()  # No instance attributes needed

    # =========================================================================
    # Helper Methods for Type Conversion
    # =========================================================================

    @staticmethod
    def _bytes_to_int(value):
        """
        Convert bytes to integer.

        Args:
            value: bytes - byte string to convert

        Returns:
            int: parsed integer value

        Raises:
            ValueError: if value is not bytes or not a valid integer
        """
        # Validate type first
        if not isinstance(value, (bytes, bytearray)):
            raise ValueError("value is not an integer or out of range")
        try:
            # Decode to string and parse as int
            # Handles both positive and negative integers
            return int(value.decode('utf-8'))
        except (ValueError, UnicodeDecodeError):
            raise ValueError("value is not an integer or out of range")

    @staticmethod
    def _int_to_bytes(value):
        """
        Convert integer to bytes.

        Args:
            value: int - integer value to convert

        Returns:
            bytes: byte representation of integer
        """
        return str(value).encode('utf-8')

    @staticmethod
    def _bytes_to_float(value):
        """
        Convert bytes to float.

        Args:
            value: bytes - byte string to convert

        Returns:
            float: parsed float value

        Raises:
            ValueError: if value is not bytes or not a valid float
        """
        # Validate type first
        if not isinstance(value, (bytes, bytearray)):
            raise ValueError("value is not a valid float")
        try:
            return float(value.decode('utf-8'))
        except (ValueError, UnicodeDecodeError):
            raise ValueError("value is not a valid float")

    @staticmethod
    def _float_to_bytes(value):
        """
        Convert float to bytes (formatted to remove unnecessary decimals).

        Args:
            value: float - float value to convert

        Returns:
            bytes: byte representation of float
        """
        # Format float: remove trailing zeros and unnecessary decimal point
        s = str(value)
        if '.' in s:
            s = s.rstrip('0').rstrip('.')
        return s.encode('utf-8')

    # =========================================================================
    # Basic String Operations
    # =========================================================================

    @staticmethod
    def append(storage, key, value):
        """
        Append value to existing string (or create if doesn't exist).

        Redis Command: APPEND key value

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to append to
            value: bytes - value to append

        Returns:
            int: length of string after append

        Raises:
            ValueError: if key exists but is not a string type
        """
        # Check if key exists and validate type
        if key in storage._data:
            # Clean expired keys first
            storage._delete_if_expired(key)

            # If still exists after expiry check, verify type
            if key in storage._data:
                actual_type = storage._types.get(key, TYPE_STRING)
                if actual_type != TYPE_STRING:
                    raise ValueError("WRONGTYPE Operation against a key holding the wrong kind of value")

        # Get existing value (handles expiry)
        existing = storage.get(key)

        if existing is None:
            # Key doesn't exist, set new value
            storage.set(key, value)
            return len(value)
        else:
            # Append to existing value (preserve TTL)
            new_value = existing + value
            storage.set_value_only(key, new_value)
            return len(new_value)

    @staticmethod
    def strlen(storage, key):
        """
        Get length of string value.

        Redis Command: STRLEN key

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to check

        Returns:
            int: length of string, or 0 if key doesn't exist
        """
        value = storage.get(key)
        if value is None:
            return 0
        return len(value)

    @staticmethod
    def getrange(storage, key, start, end):
        """
        Get substring of string value.

        Redis Command: GETRANGE key start end

        Supports negative indices (from end of string).
        If start > end after normalization, returns empty bytes.

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to read from
            start: int - start index (inclusive, can be negative)
            end: int - end index (inclusive, can be negative)

        Returns:
            bytes: substring, or empty bytes if key doesn't exist
        """
        value = storage.get(key)
        if value is None:
            return b''

        length = len(value)

        # Normalize negative indices
        if start < 0:
            start = max(0, length + start)
        if end < 0:
            end = max(-1, length + end)

        # Clamp to valid range
        start = max(0, min(start, length))
        end = max(-1, min(end, length - 1))

        # Return empty if invalid range
        if start > end:
            return b''

        # Python slicing is [start:end+1] for inclusive end
        return value[start:end + 1]

    @staticmethod
    def setrange(storage, key, offset, value):
        """
        Overwrite part of string at specified offset.

        Redis Command: SETRANGE key offset value

        If offset is beyond current string length, pads with zero bytes.
        Creates key if it doesn't exist.

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to modify
            offset: int - byte offset to start writing at
            value: bytes - value to write

        Returns:
            int: length of string after modification

        Raises:
            ValueError: if offset is negative or key is not a string
        """
        if offset < 0:
            raise ValueError("offset is out of range")

        # Get existing value or create empty
        existing = storage.get(key)
        if existing is None:
            existing = b''

        # If offset is beyond current length, pad with zero bytes
        if offset > len(existing):
            existing = existing + (b'\x00' * (offset - len(existing)))

        # Build new value with replacement
        # Parts: [0:offset] + value + [offset+len(value):]
        new_value = existing[:offset] + value

        # If there's remaining data after the inserted value, append it
        remainder_start = offset + len(value)
        if remainder_start < len(existing):
            new_value = new_value + existing[remainder_start:]

        # Preserve TTL if key existed
        if existing:
            storage.set_value_only(key, new_value)
        else:
            storage.set(key, new_value)
        return len(new_value)

    # =========================================================================
    # Numeric Operations
    # =========================================================================

    @staticmethod
    def incr(storage, key):
        """
        Increment integer value by 1.

        Redis Command: INCR key

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to increment

        Returns:
            int: value after increment

        Raises:
            ValueError: if value is not an integer
        """
        return StringOperations.incrby(storage, key, 1)

    @staticmethod
    def incrby(storage, key, increment):
        """
        Increment integer value by specified amount.

        Redis Command: INCRBY key increment

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to increment
            increment: int - amount to increment by

        Returns:
            int: value after increment

        Raises:
            ValueError: if value is not an integer
        """
        # Get existing value
        value = storage.get(key)

        if value is None:
            # Key doesn't exist, start from 0
            current = 0
        else:
            # Parse as integer
            current = StringOperations._bytes_to_int(value)

        # Increment
        new_value = current + increment

        # Store back as bytes (preserve TTL if key existed)
        new_bytes = StringOperations._int_to_bytes(new_value)
        if value is not None:
            storage.set_value_only(key, new_bytes)
        else:
            storage.set(key, new_bytes)

        return new_value

    @staticmethod
    def incrbyfloat(storage, key, increment):
        """
        Increment float value by specified amount.

        Redis Command: INCRBYFLOAT key increment

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to increment
            increment: float - amount to increment by

        Returns:
            bytes: value after increment (as string representation)

        Raises:
            ValueError: if value is not a valid float
        """
        # Get existing value
        value = storage.get(key)

        if value is None:
            # Key doesn't exist, start from 0.0
            current = 0.0
        else:
            # Parse as float (or int)
            try:
                current = StringOperations._bytes_to_float(value)
            except ValueError:
                # Try as int
                current = float(StringOperations._bytes_to_int(value))

        # Increment
        new_value = current + increment

        # Store back as bytes (preserve TTL if key existed)
        result = StringOperations._float_to_bytes(new_value)
        if value is not None:
            storage.set_value_only(key, result)
        else:
            storage.set(key, result)

        return result

    @staticmethod
    def decr(storage, key):
        """
        Decrement integer value by 1.

        Redis Command: DECR key

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to decrement

        Returns:
            int: value after decrement

        Raises:
            ValueError: if value is not an integer
        """
        return StringOperations.incrby(storage, key, -1)

    @staticmethod
    def decrby(storage, key, decrement):
        """
        Decrement integer value by specified amount.

        Redis Command: DECRBY key decrement

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to decrement
            decrement: int - amount to decrement by

        Returns:
            int: value after decrement

        Raises:
            ValueError: if value is not an integer
        """
        return StringOperations.incrby(storage, key, -decrement)

    # =========================================================================
    # Bitwise Operations
    # =========================================================================

    @staticmethod
    def setbit(storage, key, offset, value):
        """
        Set or clear bit at offset in string value.

        Redis Command: SETBIT key offset value

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to modify
            offset: int - bit offset (0-based)
            value: int - bit value (0 or 1)

        Returns:
            int: original bit value at offset (0 or 1)

        Raises:
            ValueError: if offset is negative or value is not 0/1
        """
        # Max 64KB = 524288 bits to prevent OOM on ESP32
        MAX_BIT_OFFSET = 64 * 1024 * 8

        if offset < 0:
            raise ValueError("bit offset is out of range")

        if offset > MAX_BIT_OFFSET:
            raise ValueError("bit offset is out of range")

        if value not in (0, 1):
            raise ValueError("bit value must be 0 or 1")

        # Get existing value or create empty
        existing = storage.get(key)
        if existing is None:
            existing = b''

        # Calculate byte and bit position
        byte_index = offset // 8
        bit_index = 7 - (offset % 8)  # Redis uses big-endian bit numbering

        # Extend string if necessary
        if byte_index >= len(existing):
            existing = existing + (b'\x00' * (byte_index - len(existing) + 1))

        # Convert to bytearray for modification
        data = bytearray(existing)

        # Get original bit value
        original = (data[byte_index] >> bit_index) & 1

        # Set or clear bit
        if value == 1:
            data[byte_index] |= (1 << bit_index)
        else:
            data[byte_index] &= ~(1 << bit_index)

        # Store back (preserve TTL if key existed)
        new_bytes = bytes(data)
        if existing:
            storage.set_value_only(key, new_bytes)
        else:
            storage.set(key, new_bytes)

        return original

    @staticmethod
    def getbit(storage, key, offset):
        """
        Get bit value at offset in string.

        Redis Command: GETBIT key offset

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to read from
            offset: int - bit offset (0-based)

        Returns:
            int: bit value at offset (0 or 1)

        Raises:
            ValueError: if offset is negative
        """
        if offset < 0:
            raise ValueError("bit offset is out of range")

        # Get existing value
        value = storage.get(key)
        if value is None:
            return 0

        # Calculate byte and bit position
        byte_index = offset // 8
        bit_index = 7 - (offset % 8)  # Redis uses big-endian bit numbering

        # Return 0 if offset is beyond string length
        if byte_index >= len(value):
            return 0

        # Get bit value
        return (value[byte_index] >> bit_index) & 1

    @staticmethod
    def bitcount(storage, key, start=None, end=None):
        """
        Count number of set bits (population count) in string.

        Redis Command: BITCOUNT key [start end]

        Optional start/end specify byte range (not bit range).
        Supports negative indices.

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to count bits in
            start: int | None - start byte index (inclusive)
            end: int | None - end byte index (inclusive)

        Returns:
            int: number of bits set to 1
        """
        value = storage.get(key)
        if value is None:
            return 0

        # Apply byte range if specified
        if start is not None or end is not None:
            length = len(value)

            # Default values
            if start is None:
                start = 0
            if end is None:
                end = length - 1

            # Normalize negative indices
            if start < 0:
                start = max(0, length + start)
            if end < 0:
                end = max(-1, length + end)

            # Clamp to valid range
            start = max(0, min(start, length))
            end = max(-1, min(end, length - 1))

            # Return 0 if invalid range
            if start > end:
                return 0

            # Slice to range
            value = value[start:end + 1]

        # Count bits using Brian Kernighan's algorithm
        # (More efficient than checking each bit individually)
        count = 0
        for byte in value:
            # Count set bits in this byte
            while byte:
                byte &= byte - 1  # Clear lowest set bit
                count += 1

        return count

    # =========================================================================
    # Multi-Key Operations
    # =========================================================================

    @staticmethod
    def mget(storage, *keys):
        """
        Get values of multiple keys.

        Redis Command: MGET key [key ...]

        Args:
            storage: Storage - storage engine instance
            *keys: bytes - keys to retrieve

        Returns:
            list[bytes | None]: list of values (None for non-existent keys)
        """
        result = []
        for key in keys:
            value = storage.get(key)
            result.append(value)
        return result

    @staticmethod
    def mset(storage, mapping):
        """
        Set multiple key-value pairs.

        Redis Command: MSET key value [key value ...]

        This operation is atomic - all keys are set together.

        Args:
            storage: Storage - storage engine instance
            mapping: dict - {key: value} pairs to set (bytes -> bytes)

        Returns:
            bool: always True (MSET never fails)
        """
        for key, value in mapping.items():
            storage.set(key, value)
        return True

    @staticmethod
    def msetnx(storage, mapping):
        """
        Set multiple keys only if none of them exist.

        Redis Command: MSETNX key value [key value ...]

        This operation is atomic - either all keys are set or none are.

        Args:
            storage: Storage - storage engine instance
            mapping: dict - {key: value} pairs to set (bytes -> bytes)

        Returns:
            bool: True if all keys were set, False if any key existed
        """
        # Check if any key exists
        for key in mapping.keys():
            if storage.exists(key) > 0:
                return False

        # All keys are free, set them all
        for key, value in mapping.items():
            storage.set(key, value)

        return True

    # =========================================================================
    # Additional String Commands (P0 Priority)
    # =========================================================================

    @staticmethod
    def setnx(storage, key, value):
        """
        Set key to value only if key does not exist.

        Redis Command: SETNX key value

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to set
            value: bytes - value to store

        Returns:
            int: 1 if key was set, 0 if key already exists
        """
        return 1 if storage.set(key, value, nx=True) else 0

    @staticmethod
    def setex(storage, key, seconds, value):
        """
        Set key to value with expiration in seconds.

        Redis Command: SETEX key seconds value

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to set
            seconds: int - expiration time in seconds
            value: bytes - value to store

        Returns:
            bool: True on success
        """
        return storage.set(key, value, ex=seconds)

    @staticmethod
    def psetex(storage, key, milliseconds, value):
        """
        Set key to value with expiration in milliseconds.

        Redis Command: PSETEX key milliseconds value

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to set
            milliseconds: int - expiration time in milliseconds
            value: bytes - value to store

        Returns:
            bool: True on success
        """
        return storage.set(key, value, px=milliseconds)

    @staticmethod
    def getset(storage, key, value):
        """
        Set key to value and return the old value.

        Redis Command: GETSET key value

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to set
            value: bytes - new value to store

        Returns:
            bytes | None: old value, or None if key didn't exist
        """
        old_value = storage.get(key)
        storage.set(key, value)
        return old_value

    @staticmethod
    def getdel(storage, key):
        """
        Get value of key and delete it.

        Redis Command: GETDEL key

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to get and delete

        Returns:
            bytes | None: value, or None if key didn't exist
        """
        value = storage.get(key)
        if value is not None:
            storage.delete(key)
        return value

    @staticmethod
    def getex(storage, key, ex=None, px=None, exat=None, pxat=None, persist=False):
        """
        Get value and optionally set expiration.

        Redis Command: GETEX key [EX seconds | PX ms | EXAT timestamp | PXAT ms-timestamp | PERSIST]

        Args:
            storage: Storage - storage engine instance
            key: bytes - key to get
            ex: int - set expiry in seconds
            px: int - set expiry in milliseconds
            exat: int - set expiry at Unix timestamp (seconds)
            pxat: int - set expiry at Unix timestamp (milliseconds)
            persist: bool - remove existing expiry

        Returns:
            bytes | None: value, or None if key doesn't exist
        """
        value = storage.get(key)
        if value is None:
            return None

        # Apply expiration option
        if ex is not None:
            storage.expire(key, ex)
        elif px is not None:
            storage.pexpire(key, px)
        elif exat is not None:
            storage.expireat(key, exat)
        elif pxat is not None:
            storage.pexpireat(key, pxat)
        elif persist:
            storage.persist(key)

        return value

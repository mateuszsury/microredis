"""
MicroRedis Response Builder Module

Provides pre-allocated RESP2 protocol responses and memory-efficient
response building utilities for MicroRedis on ESP32-S3.

Platform: ESP32-S3 with MicroPython
Memory-optimized with pre-allocated constants and efficient byte operations.
"""

from .constants import CRLF, SIMPLE_STRING, ERROR, INTEGER, BULK_STRING, ARRAY

# Import const() with fallback for PC-based testing
try:
    from micropython import const
except ImportError:
    const = lambda x: x

# =============================================================================
# Pre-allocated Common Responses (frozen in RAM)
# =============================================================================
# These constant responses are used frequently and are pre-built to avoid
# repeated string formatting and memory allocation at runtime.

RESP_OK = b'+OK\r\n'
RESP_PONG = b'+PONG\r\n'
RESP_NULL = b'$-1\r\n'              # Null bulk string
RESP_NULL_ARRAY = b'*-1\r\n'        # Null array
RESP_EMPTY_ARRAY = b'*0\r\n'        # Empty array
RESP_ZERO = b':0\r\n'               # Integer 0
RESP_ONE = b':1\r\n'                # Integer 1
RESP_QUEUED = b'+QUEUED\r\n'        # Transaction queued response

# =============================================================================
# Pre-allocated Error Responses
# =============================================================================
# Common error messages pre-built for performance and memory efficiency

ERR_UNKNOWN_CMD = b'-ERR unknown command\r\n'
ERR_WRONG_ARITY = b'-ERR wrong number of arguments\r\n'
ERR_WRONGTYPE = b'-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
ERR_SYNTAX = b'-ERR syntax error\r\n'
ERR_NOT_INTEGER = b'-ERR value is not an integer or out of range\r\n'
ERR_NO_KEY = b'-ERR no such key\r\n'
ERR_OUT_OF_RANGE = b'-ERR index out of range\r\n'

# Pre-allocated single-use error responses for common operations
RESP_ERR_WRONGTYPE = ERR_WRONGTYPE  # Alias for consistency
RESP_ERR_SYNTAX = ERR_SYNTAX        # Alias for consistency

# =============================================================================
# Response Building Functions
# =============================================================================
# Optimized functions for constructing RESP2 protocol responses

def simple_string(msg: str) -> bytes:
    """
    Build a RESP2 simple string response.

    Format: +<message>\r\n
    Example: simple_string('OK') -> b'+OK\r\n'

    Args:
        msg: String message to encode

    Returns:
        RESP2-encoded simple string as bytes

    Memory: Allocates new bytes object. Use pre-allocated constants when possible.
    """
    # Encode message to bytes if needed, then build RESP2 response
    if isinstance(msg, str):
        msg = msg.encode('utf-8')
    return b'+' + msg + CRLF


def error(msg: str) -> bytes:
    """
    Build a RESP2 error response.

    Format: -<error message>\r\n
    Example: error('invalid argument') -> b'-invalid argument\r\n'

    Args:
        msg: Error message to encode

    Returns:
        RESP2-encoded error as bytes

    Memory: Allocates new bytes object. Use pre-allocated error constants when possible.
    """
    if isinstance(msg, str):
        msg = msg.encode('utf-8')
    return b'-' + msg + CRLF


def error_wrongtype() -> bytes:
    """
    Return pre-allocated WRONGTYPE error response.

    Returns:
        Pre-allocated WRONGTYPE error bytes

    Memory: Zero allocation - returns constant reference
    """
    return ERR_WRONGTYPE


def error_syntax() -> bytes:
    """
    Return pre-allocated syntax error response.

    Returns:
        Pre-allocated syntax error bytes

    Memory: Zero allocation - returns constant reference
    """
    return ERR_SYNTAX


def integer(n: int) -> bytes:
    """
    Build a RESP2 integer response.

    Format: :<number>\r\n
    Example: integer(1000) -> b':1000\r\n'

    Args:
        n: Integer value to encode

    Returns:
        RESP2-encoded integer as bytes

    Optimization: Uses pre-allocated RESP_ZERO and RESP_ONE for common cases
    """
    # Use pre-allocated responses for common integers
    if n == 0:
        return RESP_ZERO
    elif n == 1:
        return RESP_ONE

    # Format integer for other values
    return b':' + str(n).encode('ascii') + CRLF


def bulk_string(data: bytes) -> bytes:
    """
    Build a RESP2 bulk string response.

    Format: $<length>\r\n<data>\r\n
    Example: bulk_string(b'hello') -> b'$5\r\nhello\r\n'

    Args:
        data: Binary data to encode (must be bytes)

    Returns:
        RESP2-encoded bulk string as bytes

    Memory: Allocates new bytes object. For large bulk strings, consider ResponseBuilder.
    """
    length = len(data)
    return b'$' + str(length).encode('ascii') + CRLF + data + CRLF


def bulk_string_or_null(data) -> bytes:
    """
    Build a RESP2 bulk string response or null if data is None.

    Format: $<length>\r\n<data>\r\n or $-1\r\n for null
    Example: bulk_string_or_null(None) -> b'$-1\r\n'
             bulk_string_or_null(b'test') -> b'$4\r\ntest\r\n'

    Args:
        data: Binary data to encode or None

    Returns:
        RESP2-encoded bulk string or null as bytes

    Optimization: Returns pre-allocated RESP_NULL when data is None
    """
    if data is None:
        return RESP_NULL
    return bulk_string(data)


def array(items: list) -> bytes:
    """
    Build a RESP2 array response from a list of pre-encoded items.

    Format: *<count>\r\n<item1><item2>...<itemN>
    Example: array([b':1\r\n', b':2\r\n']) -> b'*2\r\n:1\r\n:2\r\n'

    Args:
        items: List of already RESP2-encoded byte strings

    Returns:
        RESP2-encoded array as bytes

    Note: Items must already be RESP2-encoded. Use encode_value() to encode items.
    Memory: Allocates new bytes object. For large arrays, consider ResponseBuilder.
    """
    if not items:
        return RESP_EMPTY_ARRAY

    count = len(items)
    # Build array header then concatenate all items
    result = b'*' + str(count).encode('ascii') + CRLF
    for item in items:
        result += item
    return result


def encode_value(value) -> bytes:
    """
    Automatically encode a Python value to RESP2 protocol format.

    Type mapping:
    - None -> Null bulk string ($-1\r\n)
    - int -> Integer (:n\r\n)
    - str -> Bulk string ($len\r\ndata\r\n)
    - bytes -> Bulk string ($len\r\ndata\r\n)
    - list/tuple -> Array (*count\r\n...)

    Args:
        value: Python value to encode

    Returns:
        RESP2-encoded bytes

    Raises:
        TypeError: If value type is not supported

    Memory: Recursively allocates for nested structures. Avoid deep nesting.
    """
    # Null value
    if value is None:
        return RESP_NULL

    # Integer
    elif isinstance(value, int):
        return integer(value)

    # String - encode to bytes first
    elif isinstance(value, str):
        return bulk_string(value.encode('utf-8'))

    # Bytes - direct bulk string
    elif isinstance(value, bytes):
        return bulk_string(value)

    # List or tuple - recursive array encoding
    elif isinstance(value, (list, tuple)):
        encoded_items = [encode_value(item) for item in value]
        return array(encoded_items)

    # Unsupported type
    else:
        raise TypeError(f'Cannot encode type {type(value).__name__} to RESP2')


# =============================================================================
# ResponseBuilder Class - Efficient Multi-part Response Construction
# =============================================================================

class ResponseBuilder:
    """
    Efficient builder for complex RESP2 responses using internal bytearray.

    Uses __slots__ to minimize memory overhead and bytearray for efficient
    incremental building without repeated bytes concatenation.

    Typical usage:
        builder = ResponseBuilder()
        builder.add_array_header(3)
        builder.add_integer(1)
        builder.add_bulk(b'hello')
        builder.add_simple('OK')
        response = builder.get_response()

    Memory: Single bytearray allocation, grows as needed. More efficient than
    repeated bytes concatenation for responses with many parts.
    """
    __slots__ = ('_buffer',)

    def __init__(self, initial_capacity: int = 256):
        """
        Initialize ResponseBuilder with optional initial capacity.

        Args:
            initial_capacity: Pre-allocated buffer size in bytes (default 256)
                            Larger values reduce reallocations for big responses
        """
        self._buffer = bytearray(initial_capacity)
        # Reset length to 0 but keep capacity
        del self._buffer[:]

    def add_simple(self, msg: str) -> None:
        """
        Add a simple string response to the buffer.

        Args:
            msg: Message string to add
        """
        self._buffer.extend(b'+')
        if isinstance(msg, str):
            self._buffer.extend(msg.encode('utf-8'))
        else:
            self._buffer.extend(msg)
        self._buffer.extend(CRLF)

    def add_error(self, msg: str) -> None:
        """
        Add an error response to the buffer.

        Args:
            msg: Error message string to add
        """
        self._buffer.extend(b'-')
        if isinstance(msg, str):
            self._buffer.extend(msg.encode('utf-8'))
        else:
            self._buffer.extend(msg)
        self._buffer.extend(CRLF)

    def add_integer(self, n: int) -> None:
        """
        Add an integer response to the buffer.

        Args:
            n: Integer value to add
        """
        self._buffer.extend(b':')
        self._buffer.extend(str(n).encode('ascii'))
        self._buffer.extend(CRLF)

    def add_bulk(self, data: bytes) -> None:
        """
        Add a bulk string response to the buffer.

        Args:
            data: Binary data to add (must be bytes)
        """
        length = len(data)
        self._buffer.extend(b'$')
        self._buffer.extend(str(length).encode('ascii'))
        self._buffer.extend(CRLF)
        self._buffer.extend(data)
        self._buffer.extend(CRLF)

    def add_bulk_or_null(self, data) -> None:
        """
        Add a bulk string or null response to the buffer.

        Args:
            data: Binary data or None
        """
        if data is None:
            self._buffer.extend(RESP_NULL)
        else:
            self.add_bulk(data)

    def add_array_header(self, count: int) -> None:
        """
        Add an array header to the buffer.

        After adding header, add <count> elements using other add_* methods.

        Args:
            count: Number of array elements that will follow
        """
        self._buffer.extend(b'*')
        self._buffer.extend(str(count).encode('ascii'))
        self._buffer.extend(CRLF)

    def add_null(self) -> None:
        """Add a null bulk string response to the buffer."""
        self._buffer.extend(RESP_NULL)

    def add_raw(self, data: bytes) -> None:
        """
        Add raw pre-encoded RESP2 data to the buffer.

        Args:
            data: Already RESP2-encoded bytes
        """
        self._buffer.extend(data)

    def get_response(self) -> bytes:
        """
        Get the final response as bytes and reset the buffer.

        Returns:
            Complete RESP2 response as bytes

        Note: After calling this, the builder is reset and can be reused.
        Memory: Converts bytearray to bytes (allocates new object)
        """
        result = bytes(self._buffer)
        # Clear buffer for reuse but keep allocated capacity
        del self._buffer[:]
        return result

    def reset(self) -> None:
        """
        Reset the builder without returning data.

        Useful when building needs to be aborted or restarted.
        """
        del self._buffer[:]

    def __len__(self) -> int:
        """Return current buffer size in bytes."""
        return len(self._buffer)

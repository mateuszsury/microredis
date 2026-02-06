"""
MicroRedis Core Module

This module contains the core implementation of MicroRedis - a lightweight
Redis-compatible server implementation for MicroPython on microcontrollers.

Core components:
- Data store management
- Command parsing and execution
- Protocol handling (RESP - REdis Serialization Protocol)
- Memory-optimized data structures
- Response building and encoding

Available submodules:
- constants: Protocol constants and configuration
- protocol: RESP2 protocol parser
- response: RESP2 response builders
"""

# Export public API
from .constants import (
    # RESP2 Protocol markers
    SIMPLE_STRING,
    ERROR,
    INTEGER,
    BULK_STRING,
    ARRAY,
    # Network configuration
    DEFAULT_PORT,
    MAX_CLIENTS,
    BUFFER_SIZE,
    # Storage optimization
    ZIPLIST_MAX_ENTRIES,
    INTSET_MAX_ENTRIES,
    # TTL and expiry
    EXPIRY_CHECK_INTERVAL_MS,
    EXPIRY_SAMPLE_SIZE,
    # Protocol terminators
    CRLF,
)

from .response import (
    # Pre-allocated responses
    RESP_OK,
    RESP_PONG,
    RESP_NULL,
    RESP_NULL_ARRAY,
    RESP_EMPTY_ARRAY,
    RESP_ZERO,
    RESP_ONE,
    RESP_QUEUED,
    # Pre-allocated errors
    ERR_UNKNOWN_CMD,
    ERR_WRONG_ARITY,
    ERR_WRONGTYPE,
    ERR_SYNTAX,
    ERR_NOT_INTEGER,
    ERR_NO_KEY,
    ERR_OUT_OF_RANGE,
    # Response builders
    simple_string,
    error,
    error_wrongtype,
    error_syntax,
    integer,
    bulk_string,
    bulk_string_or_null,
    array,
    encode_value,
    ResponseBuilder,
)

__all__ = [
    # Constants
    'SIMPLE_STRING',
    'ERROR',
    'INTEGER',
    'BULK_STRING',
    'ARRAY',
    'DEFAULT_PORT',
    'MAX_CLIENTS',
    'BUFFER_SIZE',
    'ZIPLIST_MAX_ENTRIES',
    'INTSET_MAX_ENTRIES',
    'EXPIRY_CHECK_INTERVAL_MS',
    'EXPIRY_SAMPLE_SIZE',
    'CRLF',
    # Pre-allocated responses
    'RESP_OK',
    'RESP_PONG',
    'RESP_NULL',
    'RESP_NULL_ARRAY',
    'RESP_EMPTY_ARRAY',
    'RESP_ZERO',
    'RESP_ONE',
    'RESP_QUEUED',
    # Pre-allocated errors
    'ERR_UNKNOWN_CMD',
    'ERR_WRONG_ARITY',
    'ERR_WRONGTYPE',
    'ERR_SYNTAX',
    'ERR_NOT_INTEGER',
    'ERR_NO_KEY',
    'ERR_OUT_OF_RANGE',
    # Response builders
    'simple_string',
    'error',
    'error_wrongtype',
    'error_syntax',
    'integer',
    'bulk_string',
    'bulk_string_or_null',
    'array',
    'encode_value',
    'ResponseBuilder',
]

"""
MicroRedis RESP2 Protocol Parser

Streaming parser for Redis Serialization Protocol version 2 (RESP2).
Implements zero-copy parsing with state machine for efficient memory usage
on ESP32-S3 MicroPython.

Platform: ESP32-S3 with MicroPython
Memory optimization: __slots__, memoryview, pre-allocated buffers
"""

try:
    from micropython import const
except ImportError:
    const = lambda x: x

from .constants import (
    SIMPLE_STRING, ERROR, INTEGER, BULK_STRING, ARRAY, CRLF, BUFFER_SIZE,
    MAX_ARRAY_DEPTH, MAX_BULK_SIZE, MAX_ARRAY_SIZE
)

# Parser states - using const() for memory efficiency
STATE_IDLE = const(0)           # Waiting for new message
STATE_READING_TYPE = const(1)   # Reading type indicator
STATE_READING_LINE = const(2)   # Reading simple line (string/error/int)
STATE_READING_BULK = const(3)   # Reading bulk string content
STATE_READING_ARRAY = const(4)  # Reading array elements


class RESPParser:
    """
    Streaming RESP2 protocol parser with zero-copy optimization.

    Implements state machine to incrementally parse Redis protocol messages
    without blocking. Designed for memory-constrained ESP32-S3 environment.

    Usage:
        parser = RESPParser()
        parser.feed(data_chunk)
        result = parser.parse()
        if result:
            command, args = result
            # Process command

    Memory optimizations:
    - __slots__ to reduce instance overhead (~40% memory saving)
    - Pre-allocated bytearray buffer to avoid allocations
    - memoryview for zero-copy slicing
    - Minimal temporary object creation
    """

    __slots__ = (
        '_buffer',           # bytearray: Main data buffer
        '_buffer_len',       # int: Used length of buffer (avoids memoryview leaks)
        '_buffer_offset',    # int: Offset into buffer for parsing
        '_state',            # int: Current parser state
        '_type',             # int: Current RESP type being parsed
        '_bulk_len',         # int: Expected bulk string length
        '_array_len',        # int: Expected array length
        '_array_elements',   # list: Accumulated array elements
        '_line_start',       # int: Start position of current line
        '_array_depth',      # int: Current nesting depth (DoS prevention)
    )

    def __init__(self):
        """
        Initialize parser with pre-allocated buffer.

        Pre-allocates BUFFER_SIZE bytes to minimize memory fragmentation
        during operation on ESP32-S3.
        """
        # Pre-allocate buffer to avoid repeated allocations
        self._buffer = bytearray(BUFFER_SIZE)
        self._buffer_len = 0      # Track used length (avoids memoryview leak)
        self._buffer_offset = 0   # Current parse position
        self._state = STATE_IDLE
        self._type = 0
        self._bulk_len = 0
        self._array_len = 0
        self._array_elements = []
        self._line_start = 0
        self._array_depth = 0  # Track nesting depth for DoS prevention

    def feed(self, data):
        """
        Feed incoming data to the parser buffer.

        Args:
            data: bytes, bytearray, or memoryview to append to buffer

        Note:
            Uses offset tracking instead of memoryview slicing to avoid leaks.
            If buffer is full, it will be compacted by removing processed data.
        """
        if not data:
            return

        # Get current buffer length
        current_len = self._buffer_len
        new_len = current_len + len(data)

        # Check if we need to compact buffer
        if new_len > BUFFER_SIZE:
            # Move unprocessed data to start of buffer
            if self._buffer_offset > 0:
                remaining = current_len - self._buffer_offset
                self._buffer[:remaining] = self._buffer[self._buffer_offset:current_len]
                current_len = remaining
                self._buffer_offset = 0
                self._line_start = 0
                new_len = current_len + len(data)

        # Ensure buffer has capacity
        if new_len > len(self._buffer):
            # Extend buffer capacity
            self._buffer.extend(b'\x00' * (new_len - len(self._buffer)))

        # Copy new data into buffer
        self._buffer[current_len:new_len] = data
        self._buffer_len = new_len

    def parse(self):
        """
        Parse buffered data and extract next complete message.

        Returns:
            tuple: (command, args) if complete message parsed
                   command: str - Command name (uppercase)
                   args: list - Command arguments as bytes
            None: If message incomplete, need more data

        Raises:
            ValueError: If protocol violation detected

        State machine handles incremental parsing without blocking.
        """
        while True:
            # Calculate available data from current offset
            available = self._buffer_len - self._buffer_offset

            if self._state == STATE_IDLE:
                # Start parsing new message
                if available == 0:
                    return None

                self._state = STATE_READING_TYPE
                self._line_start = self._buffer_offset

            elif self._state == STATE_READING_TYPE:
                # Read type indicator (first byte)
                if available < 1:
                    return None

                self._type = self._buffer[self._buffer_offset]

                # Check if this is an inline command (not starting with RESP type marker)
                # RESP type markers: * $ + - :
                if self._type not in (SIMPLE_STRING, ERROR, INTEGER, BULK_STRING, ARRAY):
                    # Inline command: parse as space-separated line
                    result = self._parse_inline_command()
                    if result is not None:
                        return result
                    # Need more data
                    return None

                self._line_start = self._buffer_offset + 1

                if self._type == SIMPLE_STRING or self._type == ERROR or self._type == INTEGER:
                    self._state = STATE_READING_LINE
                elif self._type == BULK_STRING:
                    self._state = STATE_READING_LINE  # Read length first
                elif self._type == ARRAY:
                    self._state = STATE_READING_LINE  # Read array length
                    self._array_elements = []
                    # DoS prevention: track array depth
                    self._array_depth += 1
                    if self._array_depth > MAX_ARRAY_DEPTH:
                        raise ValueError(f"Array nesting too deep: {self._array_depth} > {MAX_ARRAY_DEPTH}")

            elif self._state == STATE_READING_LINE:
                # Read until CRLF
                crlf_pos = self._find_crlf(self._line_start)
                if crlf_pos == -1:
                    return None  # Need more data

                # Extract line content (without CRLF)
                line = bytes(self._buffer[self._line_start:crlf_pos])

                # Process based on type
                if self._type == SIMPLE_STRING:
                    result = self._complete_message(line)
                    self._consume_bytes(crlf_pos + 2)
                    return result

                elif self._type == ERROR:
                    result = self._complete_message(line)
                    self._consume_bytes(crlf_pos + 2)
                    return result

                elif self._type == INTEGER:
                    result = self._complete_message(int(line))
                    self._consume_bytes(crlf_pos + 2)
                    return result

                elif self._type == BULK_STRING:
                    # Parse bulk string length
                    self._bulk_len = int(line)

                    # Handle NULL bulk string
                    if self._bulk_len == -1:
                        result = self._complete_message(None)
                        self._consume_bytes(crlf_pos + 2)
                        return result

                    # DoS prevention: validate negative bulk strings
                    if self._bulk_len < -1:
                        raise ValueError(f"Invalid bulk string length: {self._bulk_len}")

                    # DoS prevention: limit bulk string size
                    if self._bulk_len > MAX_BULK_SIZE:
                        raise ValueError(f"Bulk string too large: {self._bulk_len} > {MAX_BULK_SIZE}")

                    # Prepare to read bulk content
                    self._line_start = crlf_pos + 2
                    self._state = STATE_READING_BULK

                elif self._type == ARRAY:
                    # Parse array length
                    self._array_len = int(line)
                    self._consume_bytes(crlf_pos + 2)

                    # DoS prevention: limit array size
                    if self._array_len > MAX_ARRAY_SIZE:
                        raise ValueError(f"Array too large: {self._array_len} > {MAX_ARRAY_SIZE}")

                    if self._array_len == 0:
                        self._array_depth = max(0, self._array_depth - 1)
                        result = self._complete_message([])
                        return result

                    # Start reading array elements
                    self._state = STATE_READING_ARRAY

            elif self._state == STATE_READING_BULK:
                # Read bulk string content + CRLF
                needed = self._bulk_len + 2  # +2 for CRLF
                available_for_bulk = self._buffer_len - self._line_start

                if available_for_bulk < needed:
                    return None  # Need more data

                # Extract bulk content (without trailing CRLF)
                bulk_data = bytes(self._buffer[self._line_start:self._line_start + self._bulk_len])

                # If part of array, accumulate
                if self._array_elements is not None and len(self._array_elements) < self._array_len:
                    self._array_elements.append(bulk_data)
                    self._consume_bytes(self._line_start + needed)

                    # Check if array complete
                    if len(self._array_elements) == self._array_len:
                        self._array_depth = max(0, self._array_depth - 1)
                        result = self._complete_array()
                        return result
                    else:
                        # Read next array element
                        self._state = STATE_READING_TYPE
                else:
                    # Standalone bulk string
                    result = self._complete_message(bulk_data)
                    self._consume_bytes(self._line_start + needed)
                    return result

            elif self._state == STATE_READING_ARRAY:
                # Recursively parse array elements
                # Reset to read next element
                self._state = STATE_READING_TYPE

    def reset(self):
        """
        Reset parser to initial state.

        Clears buffer and resets all state variables.
        Call this after connection error or to start fresh parsing.
        """
        self._buffer_len = 0
        self._buffer_offset = 0
        self._state = STATE_IDLE
        self._type = 0
        self._bulk_len = 0
        self._array_len = 0
        self._array_elements = []
        self._line_start = 0
        self._array_depth = 0

    def _find_crlf(self, start=0):
        """
        Find position of CRLF in buffer starting from offset.

        Args:
            start: int - Starting position to search from

        Returns:
            int: Position of CR (start of CRLF), or -1 if not found

        Searches directly in bytearray to avoid memory allocation.
        """
        buf = self._buffer
        end = self._buffer_len - 1  # Need at least 2 bytes for CRLF
        i = start
        while i < end:
            if buf[i] == 13 and buf[i + 1] == 10:  # \r\n
                return i
            i += 1
        return -1

    def _consume_bytes(self, up_to):
        """
        Mark bytes as consumed by advancing offset.

        Args:
            up_to: int - Consume bytes up to this position (exclusive)

        Uses offset tracking instead of memoryview slicing to avoid leaks.
        """
        self._buffer_offset = up_to
        self._line_start = up_to
        self._state = STATE_IDLE

    def _complete_message(self, value):
        """
        Complete simple message parsing (non-array).

        Args:
            value: Parsed value (bytes, int, str, or None)

        Returns:
            tuple: (command, args) format for compatibility
        """
        self._state = STATE_IDLE
        return (value, [])

    def _complete_array(self):
        """
        Complete array message parsing.

        Returns:
            tuple: (command, args) where command is first element (uppercase)
                   and args are remaining elements

        Redis commands are sent as arrays: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
        """
        elements = self._array_elements
        self._array_elements = []
        self._state = STATE_IDLE

        if not elements:
            return (None, [])

        # First element is command (convert to uppercase string)
        try:
            command = elements[0].decode('utf-8').upper() if elements[0] else None
        except UnicodeDecodeError:
            # Invalid UTF-8 in command name - treat as invalid command
            return (None, [])
        args = elements[1:] if len(elements) > 1 else []

        return (command, args)

    def _parse_inline_command(self):
        """
        Parse inline command (telnet-style, without RESP framing).

        Inline commands are space-separated: PING\r\n or SET key value\r\n

        Supports quoted strings for arguments with spaces:
        - Single quotes: SET key 'hello world'
        - Double quotes: SET key "hello world"

        Returns:
            tuple: (command, args) if complete command parsed
            None: If command incomplete, need more data

        Raises:
            ValueError: If quoted string is not properly closed

        Note:
            This is called when the first byte is not a RESP type marker.
            Used for redis-cli telnet mode and simple debugging tools.
        """
        # Find end of line (CRLF or LF)
        crlf_pos = self._find_crlf(self._buffer_offset)
        if crlf_pos == -1:
            # Also check for just LF (some clients use that)
            lf_pos = bytes(self._buffer[self._buffer_offset:self._buffer_len]).find(b'\n')
            if lf_pos == -1:
                return None  # Need more data
            crlf_pos = self._buffer_offset + lf_pos
            line_end = crlf_pos + 1
        else:
            line_end = crlf_pos + 2

        # Extract line content (without line ending)
        line = bytes(self._buffer[self._buffer_offset:crlf_pos])

        # Consume the line
        self._consume_bytes(line_end)

        # Handle empty line
        if not line or line.isspace():
            return None

        # Parse into tokens (handle quoted strings)
        tokens = self._tokenize_inline(line)

        if not tokens:
            return None

        # First token is command (uppercase)
        try:
            command = tokens[0].decode('utf-8').upper() if tokens[0] else None
        except UnicodeDecodeError:
            # Invalid UTF-8 in command name - treat as invalid command
            return (None, [])
        args = tokens[1:] if len(tokens) > 1 else []

        return (command, args)

    def _tokenize_inline(self, line):
        """
        Tokenize inline command line respecting quotes.

        Args:
            line: bytes - The command line to tokenize

        Returns:
            list: List of token bytes

        Raises:
            ValueError: If quoted string is not properly closed
        """
        tokens = []
        i = 0
        n = len(line)

        while i < n:
            # Skip whitespace
            while i < n and line[i] in (ord(b' '), ord(b'\t')):
                i += 1

            if i >= n:
                break

            # Check for quoted string
            if line[i] == ord(b'"') or line[i] == ord(b"'"):
                quote_char = line[i]
                i += 1
                start = i

                # Find closing quote
                while i < n and line[i] != quote_char:
                    # Handle escape sequence
                    if line[i] == ord(b'\\') and i + 1 < n:
                        i += 2
                    else:
                        i += 1

                # Check for unclosed quote
                if i >= n or line[i] != quote_char:
                    raise ValueError("Protocol error: unclosed quoted string")

                # Extract token (without quotes)
                token = line[start:i]
                # Unescape basic sequences (backslash FIRST via placeholder to avoid double-unescape)
                token = token.replace(b'\\\\', b'\x00BSLASH\x00')
                token = token.replace(b'\\n', b'\n').replace(b'\\r', b'\r').replace(b'\\t', b'\t')
                token = token.replace(b'\\"', b'"').replace(b"\\'", b"'")
                token = token.replace(b'\x00BSLASH\x00', b'\\')
                tokens.append(token)

                # Skip closing quote
                i += 1
            else:
                # Unquoted token - read until whitespace
                start = i
                while i < n and line[i] not in (ord(b' '), ord(b'\t')):
                    i += 1
                tokens.append(line[start:i])

        return tokens

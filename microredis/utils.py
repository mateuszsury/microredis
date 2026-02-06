"""
MicroRedis Utility Functions

Provides common utility functions for pattern matching, parsing, and
other shared functionality. Optimized for ESP32-S3 MicroPython.

Platform: ESP32-S3 with MicroPython
RAM Constraints: ~300KB available for application
"""


def glob_match(pattern, text):
    """
    Match text against glob-style pattern (Redis KEYS/SCAN pattern matching).

    Supports:
    - * : matches any sequence of characters (including empty)
    - ? : matches exactly one character
    - [abc] : matches one character from the set
    - [a-z] : matches one character from the range
    - [^abc] : matches one character NOT in the set
    - \\ : escapes the next character (literal match)

    Args:
        pattern: bytes or str - Glob pattern
        text: bytes or str - Text to match

    Returns:
        bool: True if text matches pattern

    Note:
        Both pattern and text should be the same type (both bytes or both str).
        For bytes input, pattern characters are compared as byte values.
    """
    # Convert to common format if needed
    if isinstance(pattern, str):
        pattern = pattern.encode('utf-8')
    if isinstance(text, str):
        text = text.encode('utf-8')

    # Empty pattern only matches empty text
    if not pattern:
        return not text

    pi = 0  # pattern index
    ti = 0  # text index
    star_pi = -1  # position after last *
    star_ti = -1  # position in text when * was found

    while ti < len(text):
        if pi < len(pattern):
            pc = pattern[pi]

            if pc == ord(b'*'):
                # Wildcard - save position for backtracking
                star_pi = pi + 1
                star_ti = ti
                pi += 1
                continue

            elif pc == ord(b'?'):
                # Single character wildcard
                pi += 1
                ti += 1
                continue

            elif pc == ord(b'['):
                # Character class
                matched, class_end = _match_char_class(pattern, pi, text[ti])
                if matched:
                    pi = class_end
                    ti += 1
                    continue
                # No match, try backtracking

            elif pc == ord(b'\\'):
                # Escape character - match next char literally
                pi += 1
                if pi < len(pattern) and pattern[pi] == text[ti]:
                    pi += 1
                    ti += 1
                    continue
                # No match, try backtracking

            elif pc == text[ti]:
                # Literal character match
                pi += 1
                ti += 1
                continue

        # No match at current position - try backtracking to last *
        if star_pi != -1:
            pi = star_pi
            star_ti += 1
            ti = star_ti
        else:
            return False

    # Skip any trailing * in pattern
    while pi < len(pattern) and pattern[pi] == ord(b'*'):
        pi += 1

    # Match if both pattern and text fully consumed
    return pi == len(pattern)


def _match_char_class(pattern, start, char):
    """
    Match character against character class [abc] or [a-z] or [^abc].

    Args:
        pattern: bytes - Pattern
        start: int - Position of '[' in pattern
        char: int - Character byte value to match

    Returns:
        tuple: (matched: bool, end_position: int)
               end_position is position after ']' if matched, start if not
    """
    # Find closing bracket
    end = start + 1
    while end < len(pattern):
        if pattern[end] == ord(b']'):
            break
        if pattern[end] == ord(b'\\') and end + 1 < len(pattern):
            end += 2  # Skip escaped character
        else:
            end += 1

    if end >= len(pattern):
        # Unclosed bracket - treat as literal
        return (pattern[start] == char, start + 1)

    i = start + 1
    negate = False

    # Check for negation [^abc]
    if i < end and pattern[i] == ord(b'^'):
        negate = True
        i += 1

    matched = False

    while i < end:
        # Handle escape sequence
        if pattern[i] == ord(b'\\') and i + 1 < end:
            i += 1
            if pattern[i] == char:
                matched = True
            i += 1
            continue

        # Handle range [a-z]
        if i + 2 < end and pattern[i + 1] == ord(b'-'):
            range_start = pattern[i]
            range_end = pattern[i + 2]

            # Handle escaped range end
            if pattern[i + 2] == ord(b'\\') and i + 3 < end:
                range_end = pattern[i + 3]
                i += 1

            if range_start <= char <= range_end:
                matched = True

            i += 3
            continue

        # Single character
        if pattern[i] == char:
            matched = True

        i += 1

    # Apply negation
    if negate:
        matched = not matched

    return (matched, end + 1)


def parse_int(value, default=None):
    """
    Parse integer value from bytes or string.

    Args:
        value: bytes, str, or int - Value to parse
        default: Any - Default value if parsing fails

    Returns:
        int or default value
    """
    if isinstance(value, int):
        return value

    try:
        if isinstance(value, bytes):
            return int(value.decode('utf-8'))
        return int(value)
    except (ValueError, UnicodeDecodeError, AttributeError):
        return default


def parse_float(value, default=None):
    """
    Parse float value from bytes or string.

    Args:
        value: bytes, str, or float - Value to parse
        default: Any - Default value if parsing fails

    Returns:
        float or default value
    """
    if isinstance(value, (int, float)):
        return float(value)

    try:
        if isinstance(value, bytes):
            return float(value.decode('utf-8'))
        return float(value)
    except (ValueError, UnicodeDecodeError, AttributeError):
        return default


def to_bytes(value):
    """
    Convert value to bytes.

    Args:
        value: Any - Value to convert

    Returns:
        bytes: Value as bytes
    """
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode('utf-8')
    if isinstance(value, (int, float)):
        return str(value).encode('utf-8')
    if value is None:
        return b''
    return bytes(value)


def to_str(value, encoding='utf-8'):
    """
    Convert value to string.

    Args:
        value: Any - Value to convert
        encoding: str - Encoding for bytes (default: utf-8)

    Returns:
        str: Value as string
    """
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        return value.decode(encoding)
    if value is None:
        return ''
    return str(value)


def split_args(args, count):
    """
    Split command arguments into fixed and variable parts.

    Args:
        args: list - Command arguments
        count: int - Number of fixed arguments

    Returns:
        tuple: (fixed_args, remaining_args)
    """
    return args[:count], args[count:]


def pairs_to_dict(pairs):
    """
    Convert flat list of pairs to dictionary.

    Args:
        pairs: list - [key1, val1, key2, val2, ...]

    Returns:
        dict: {key1: val1, key2: val2, ...}
    """
    return {pairs[i]: pairs[i + 1] for i in range(0, len(pairs), 2)}


def dict_to_pairs(d):
    """
    Convert dictionary to flat list of pairs.

    Args:
        d: dict - Dictionary to convert

    Returns:
        list: [key1, val1, key2, val2, ...]
    """
    result = []
    for k, v in d.items():
        result.append(k)
        result.append(v)
    return result


def normalize_range_index(index, length):
    """
    Normalize a range index for Redis list/string operations.

    Handles negative indices (counting from end).

    Args:
        index: int - Index to normalize (can be negative)
        length: int - Length of the collection

    Returns:
        int: Normalized index (clamped to valid range)
    """
    if index < 0:
        index = length + index

    return max(0, min(index, length - 1)) if length > 0 else 0


def get_timestamp_ms():
    """
    Get current timestamp in milliseconds.

    Uses time.ticks_ms() on MicroPython for efficiency.

    Returns:
        int: Timestamp in milliseconds
    """
    try:
        import time
        return time.ticks_ms()
    except AttributeError:
        # CPython fallback
        import time
        return int(time.time() * 1000)


def get_timestamp_us():
    """
    Get current timestamp in microseconds.

    Uses time.ticks_us() on MicroPython for efficiency.

    Returns:
        int: Timestamp in microseconds
    """
    try:
        import time
        return time.ticks_us()
    except AttributeError:
        # CPython fallback
        import time
        return int(time.time() * 1000000)

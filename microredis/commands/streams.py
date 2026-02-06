"""
MicroRedis Stream Operations Module

Redis Streams implementation for MicroRedis on ESP32-S3 with MicroPython.
Implements core stream commands: XADD, XLEN, XRANGE, XREVRANGE, XREAD, XTRIM.

Platform: ESP32-S3 with MicroPython
RAM Constraints: ~300KB available for application

Stream Entry ID Format:
- <millisecondsTime>-<sequenceNumber>
- Example: 1526919030474-0
- Auto-generated when ID is '*'

Memory Optimizations:
- All methods are static (no instance data)
- Stream stored as dict with ordered list of entries
- Efficient ID comparison using tuple comparison
- Minimal temporary allocations
- Direct integration with storage engine
"""

import time

from microredis.storage.engine import TYPE_STREAM


class StreamOperations:
    """
    Redis Streams operations for MicroRedis.

    All methods are static and operate directly on the storage engine.
    Designed for minimal memory overhead on ESP32-S3.

    Stream Structure in Storage:
    {
        'entries': [(id_str, {field: value, ...}), ...],  # Ordered by ID
        'last_id': '0-0',  # Last inserted ID
        'length': 0,       # Number of entries
    }
    """

    __slots__ = ()  # No instance attributes needed

    # =========================================================================
    # Helper Methods for Stream Management
    # =========================================================================

    @staticmethod
    def _get_stream(storage, key):
        """
        Get stream structure from storage or create new empty stream.

        Args:
            storage: Storage - storage engine instance
            key: bytes - stream key

        Returns:
            dict: stream structure with entries, last_id, length
        """
        # Check if key exists
        if key not in storage._data:
            # Return new empty stream
            return {
                'entries': [],
                'last_id': '0-0',
                'length': 0,
            }

        # Check expiry
        if storage._delete_if_expired(key):
            return {
                'entries': [],
                'last_id': '0-0',
                'length': 0,
            }

        # Get existing stream data
        stream = storage._data.get(key)

        # Validate it's a stream type
        if not isinstance(stream, dict) or 'entries' not in stream:
            # Key exists but wrong type
            raise ValueError("WRONGTYPE Operation against a key holding the wrong kind of value")

        return stream

    @staticmethod
    def _set_stream(storage, key, stream):
        """
        Store stream structure back to storage.

        Args:
            storage: Storage - storage engine instance
            key: bytes - stream key
            stream: dict - stream structure
        """
        storage._data[key] = stream

        # Mark as stream type in storage for WRONGTYPE checking
        storage._types[key] = TYPE_STREAM

        # Increment version for WATCH
        storage._increment_version(key)

    @staticmethod
    def _parse_id(id_bytes):
        """
        Parse stream entry ID into (milliseconds, sequence) tuple.

        Args:
            id_bytes: bytes - stream ID in format "ms-seq"

        Returns:
            tuple: (milliseconds: int, sequence: int)

        Raises:
            ValueError: if ID format is invalid
        """
        try:
            id_str = id_bytes.decode('utf-8') if isinstance(id_bytes, bytes) else id_bytes
            parts = id_str.split('-')
            if len(parts) != 2:
                raise ValueError("Invalid stream ID format")
            return (int(parts[0]), int(parts[1]))
        except (ValueError, AttributeError, UnicodeDecodeError):
            raise ValueError("Invalid stream ID format")

    @staticmethod
    def _id_to_string(id_bytes):
        """
        Convert ID bytes to string.

        Args:
            id_bytes: bytes - stream ID

        Returns:
            str: ID as string
        """
        return id_bytes.decode('utf-8') if isinstance(id_bytes, bytes) else id_bytes

    @staticmethod
    def _id_greater(id1, id2):
        """
        Check if id1 > id2.

        Args:
            id1: bytes - first stream ID
            id2: bytes - second stream ID

        Returns:
            bool: True if id1 > id2
        """
        try:
            tuple1 = StreamOperations._parse_id(id1)
            tuple2 = StreamOperations._parse_id(id2)
            return tuple1 > tuple2
        except ValueError:
            return False

    @staticmethod
    def _id_in_range(id_bytes, start, end):
        """
        Check if ID is within range [start, end] inclusive.

        Args:
            id_bytes: bytes - stream ID to check
            start: bytes - range start ID
            end: bytes - range end ID

        Returns:
            bool: True if start <= id_bytes <= end
        """
        try:
            id_tuple = StreamOperations._parse_id(id_bytes)
            start_tuple = StreamOperations._parse_id(start)
            end_tuple = StreamOperations._parse_id(end)
            return start_tuple <= id_tuple <= end_tuple
        except ValueError:
            return False

    @staticmethod
    def _generate_id(stream, requested_id=None):
        """
        Generate next stream entry ID.

        Args:
            stream: dict - stream structure
            requested_id: bytes | None - specific ID requested (or b'*' for auto)

        Returns:
            str: generated ID string

        Raises:
            ValueError: if requested ID is not greater than last ID
        """
        if requested_id == b'*' or requested_id is None:
            # Auto-generate ID based on current time
            ms = int(time.time() * 1000)
            seq = 0

            if stream['last_id'] != '0-0':
                last_ms, last_seq = StreamOperations._parse_id(stream['last_id'].encode())

                # If same millisecond, increment sequence
                if ms == last_ms:
                    seq = last_seq + 1
                elif ms < last_ms:
                    # Clock went backwards, use last_ms and increment seq
                    ms = last_ms
                    seq = last_seq + 1

            return f'{ms}-{seq}'
        else:
            # Use requested ID but validate it's greater than last
            id_str = StreamOperations._id_to_string(requested_id)
            return id_str

    # =========================================================================
    # Stream Operations - XADD
    # =========================================================================

    @staticmethod
    def xadd(storage, key, entry_id, fields):
        """
        Append entry to stream.

        Redis Command: XADD key ID field value [field value ...]

        Args:
            storage: Storage - storage engine instance
            key: bytes - stream key
            entry_id: bytes - entry ID ('*' for auto-generate or 'ms-seq')
            fields: dict - {field: value} pairs (bytes -> bytes)

        Returns:
            bytes: generated entry ID

        Raises:
            ValueError: if ID is not greater than last ID or invalid format
        """
        stream = StreamOperations._get_stream(storage, key)

        # Generate or validate ID
        if entry_id == b'*':
            id_str = StreamOperations._generate_id(stream)
        else:
            id_str = StreamOperations._id_to_string(entry_id)

            # Validate ID is greater than last
            if stream['last_id'] != '0-0':
                if not StreamOperations._id_greater(id_str.encode(), stream['last_id'].encode()):
                    raise ValueError("ERR The ID specified in XADD is equal or smaller than the target stream top item")

        # Validate ID format
        try:
            StreamOperations._parse_id(id_str.encode())
        except ValueError:
            raise ValueError("ERR Invalid stream ID specified as stream command argument")

        # Create entry (convert bytes keys/values to strings for storage)
        entry_dict = {}
        for field, value in fields.items():
            field_str = field.decode('utf-8') if isinstance(field, bytes) else field
            value_str = value.decode('utf-8') if isinstance(value, bytes) else value
            entry_dict[field_str] = value_str

        # Add entry to stream
        stream['entries'].append((id_str, entry_dict))
        stream['last_id'] = id_str
        stream['length'] += 1

        # Store back
        StreamOperations._set_stream(storage, key, stream)

        return id_str.encode()

    # =========================================================================
    # Stream Operations - XLEN
    # =========================================================================

    @staticmethod
    def xlen(storage, key):
        """
        Get number of entries in stream.

        Redis Command: XLEN key

        Args:
            storage: Storage - storage engine instance
            key: bytes - stream key

        Returns:
            int: number of entries (0 if stream doesn't exist)
        """
        try:
            stream = StreamOperations._get_stream(storage, key)
            return stream['length']
        except ValueError:
            # Wrong type
            raise

    # =========================================================================
    # Stream Operations - XRANGE
    # =========================================================================

    @staticmethod
    def xrange(storage, key, start, end, count=None):
        """
        Get range of entries from stream (oldest to newest).

        Redis Command: XRANGE key start end [COUNT count]

        Special IDs:
        - '-' : minimum ID (0-0)
        - '+' : maximum ID (18446744073709551615-18446744073709551615)

        Args:
            storage: Storage - storage engine instance
            key: bytes - stream key
            start: bytes - start ID (inclusive, or '-')
            end: bytes - end ID (inclusive, or '+')
            count: int | None - maximum number of entries to return

        Returns:
            list: [(id_bytes, {field: value}), ...] ordered oldest to newest
        """
        stream = StreamOperations._get_stream(storage, key)

        if not stream['entries']:
            return []

        # Parse special IDs
        if start == b'-':
            start = b'0-0'
        if end == b'+':
            # Maximum possible ID
            end = b'18446744073709551615-18446744073709551615'

        result = []
        for entry_id_str, entry_fields in stream['entries']:
            entry_id = entry_id_str.encode()

            if StreamOperations._id_in_range(entry_id, start, end):
                # Convert fields back to bytes
                fields_bytes = {}
                for field, value in entry_fields.items():
                    fields_bytes[field.encode('utf-8')] = value.encode('utf-8')

                result.append((entry_id, fields_bytes))

                if count and len(result) >= count:
                    break

        return result

    # =========================================================================
    # Stream Operations - XREVRANGE
    # =========================================================================

    @staticmethod
    def xrevrange(storage, key, end, start, count=None):
        """
        Get range of entries from stream in reverse order (newest to oldest).

        Redis Command: XREVRANGE key end start [COUNT count]

        Note: Arguments are reversed compared to XRANGE (end comes first).

        Special IDs:
        - '-' : minimum ID (0-0)
        - '+' : maximum ID (18446744073709551615-18446744073709551615)

        Args:
            storage: Storage - storage engine instance
            key: bytes - stream key
            end: bytes - end ID (inclusive, or '+')
            start: bytes - start ID (inclusive, or '-')
            count: int | None - maximum number of entries to return

        Returns:
            list: [(id_bytes, {field: value}), ...] ordered newest to oldest
        """
        stream = StreamOperations._get_stream(storage, key)

        if not stream['entries']:
            return []

        # Parse special IDs
        if start == b'-':
            start = b'0-0'
        if end == b'+':
            end = b'18446744073709551615-18446744073709551615'

        result = []
        # Iterate in reverse
        for entry_id_str, entry_fields in reversed(stream['entries']):
            entry_id = entry_id_str.encode()

            if StreamOperations._id_in_range(entry_id, start, end):
                # Convert fields back to bytes
                fields_bytes = {}
                for field, value in entry_fields.items():
                    fields_bytes[field.encode('utf-8')] = value.encode('utf-8')

                result.append((entry_id, fields_bytes))

                if count and len(result) >= count:
                    break

        return result

    # =========================================================================
    # Stream Operations - XREAD
    # =========================================================================

    @staticmethod
    def xread(storage, streams_dict, count=None, block=None):
        """
        Read entries from multiple streams (simplified non-blocking version).

        Redis Command: XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] ID [ID ...]

        Args:
            storage: Storage - storage engine instance
            streams_dict: dict - {key: last_id} mapping (bytes -> bytes)
            count: int | None - max entries per stream
            block: int | None - blocking timeout in ms (NOT IMPLEMENTED - always returns immediately)

        Returns:
            list | None: [(key, [(id, fields), ...]), ...] or None if no new entries

        Note: This is a simplified implementation that does not support blocking.
        On ESP32-S3, blocking would require async integration with event loop.
        """
        # Note: block parameter is accepted but ignored (non-blocking only)
        if block is not None:
            # For production, could integrate with uasyncio for blocking behavior
            pass

        result = []

        for key, last_id in streams_dict.items():
            try:
                stream = StreamOperations._get_stream(storage, key)
            except ValueError:
                # Wrong type, skip this stream
                continue

            entries = []

            for entry_id_str, entry_fields in stream['entries']:
                entry_id = entry_id_str.encode()

                # Check if this entry is after the requested last_id
                # Special case: '>' means all entries newer than last in stream
                if last_id == b'>':
                    # Not applicable for XREAD (only for XREADGROUP)
                    # For XREAD, '>' is invalid, skip
                    break

                if StreamOperations._id_greater(entry_id, last_id):
                    # Convert fields back to bytes
                    fields_bytes = {}
                    for field, value in entry_fields.items():
                        fields_bytes[field.encode('utf-8')] = value.encode('utf-8')

                    entries.append((entry_id, fields_bytes))

                    if count and len(entries) >= count:
                        break

            if entries:
                result.append((key, entries))

        return result if result else None

    # =========================================================================
    # Stream Operations - XTRIM
    # =========================================================================

    @staticmethod
    def xtrim(storage, key, maxlen, approximate=False):
        """
        Trim stream to specified maximum length.

        Redis Command: XTRIM key MAXLEN [~] count

        Args:
            storage: Storage - storage engine instance
            key: bytes - stream key
            maxlen: int - maximum number of entries to keep
            approximate: bool - if True, use approximate trimming (ignored for now)

        Returns:
            int: number of entries deleted

        Note: Approximate trimming is not implemented - always does exact trimming.
        """
        try:
            stream = StreamOperations._get_stream(storage, key)
        except ValueError:
            # Wrong type
            raise

        if stream['length'] <= maxlen:
            return 0

        # Calculate how many entries to remove
        trimmed = stream['length'] - maxlen

        # Keep only the last 'maxlen' entries
        stream['entries'] = stream['entries'][-maxlen:]
        stream['length'] = maxlen

        # Redis never resets last_id even when stream is emptied

        # Store back
        StreamOperations._set_stream(storage, key, stream)

        return trimmed

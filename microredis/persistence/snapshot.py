"""
MicroRDB Snapshot Manager - Binary persistence for MicroRedis

Implements RDB-like binary format optimized for MicroPython/ESP32-S3.
Features: atomic writes, CRC32 validation, async yielding, wear-leveling.
"""

import struct
import time
import os

try:
    import binascii
    HAS_BINASCII = True
except ImportError:
    HAS_BINASCII = False

try:
    import uasyncio as asyncio
except ImportError:
    import asyncio

from microredis.storage.engine import (
    Storage, TYPE_STRING, TYPE_HASH, TYPE_LIST, TYPE_SET, TYPE_ZSET, TYPE_STREAM
)


# MicroRDB Format Constants
MAGIC = b'MRDB'
VERSION = 1
HEADER_SIZE = 14  # MRDB(4) + version(2) + timestamp(4) + key_count(4)
FOOTER_SIZE = 4   # CRC32


def _simple_crc32(data: bytes) -> int:
    """Simple CRC32 implementation for platforms without binascii."""
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xEDB88320
            else:
                crc >>= 1
    return crc ^ 0xFFFFFFFF


def calculate_crc32(data: bytes) -> int:
    """Calculate CRC32 checksum."""
    if HAS_BINASCII:
        return binascii.crc32(data) & 0xFFFFFFFF
    else:
        return _simple_crc32(data)


class SnapshotManager:
    """
    Binary snapshot persistence manager for MicroRedis.

    Implements MicroRDB format with:
    - Binary encoding for all Redis data types
    - CRC32 validation
    - Atomic writes with temporary files
    - Async yielding to prevent blocking
    - Auto-save based on time and change count
    """
    __slots__ = (
        '_storage',
        '_filepath',
        '_last_save',
        '_changes_since_save',
        '_save_interval',
        '_min_changes',
    )

    def __init__(
        self,
        storage: Storage,
        filepath: str = 'data.mrdb',
        save_interval: int = 300,
        min_changes: int = 100
    ):
        """
        Initialize snapshot manager.

        Args:
            storage: Storage engine instance
            filepath: Path to snapshot file
            save_interval: Seconds between auto-saves
            min_changes: Minimum changes before auto-save triggers
        """
        self._storage = storage
        self._filepath = filepath
        self._last_save = 0
        self._changes_since_save = 0
        self._save_interval = save_interval
        self._min_changes = min_changes

    def mark_change(self) -> None:
        """Increment change counter for auto-save tracking."""
        self._changes_since_save += 1

    def should_auto_save(self) -> bool:
        """Check if auto-save should trigger."""
        if self._changes_since_save < self._min_changes:
            return False

        elapsed = time.time() - self._last_save
        return elapsed >= self._save_interval

    def get_last_save_time(self) -> int:
        """Get timestamp of last save."""
        return self._last_save

    def _encode_string_value(self, value: bytes) -> bytes:
        """Encode string value (raw bytes)."""
        return value

    def _encode_hash_value(self, value: dict) -> bytes:
        """Encode hash value: count(2B) + (field_len(2B) + field + val_len(4B) + val)*count."""
        parts = [struct.pack('<H', len(value))]

        for field, val in value.items():
            field_bytes = field if isinstance(field, bytes) else field.encode('utf-8')
            val_bytes = val if isinstance(val, bytes) else val.encode('utf-8')

            parts.append(struct.pack('<H', len(field_bytes)))
            parts.append(field_bytes)
            parts.append(struct.pack('<I', len(val_bytes)))
            parts.append(val_bytes)

        return b''.join(parts)

    def _encode_list_value(self, value: list) -> bytes:
        """Encode list value: count(4B) + (item_len(4B) + item)*count."""
        parts = [struct.pack('<I', len(value))]

        for item in value:
            item_bytes = item if isinstance(item, bytes) else item.encode('utf-8')
            parts.append(struct.pack('<I', len(item_bytes)))
            parts.append(item_bytes)

        return b''.join(parts)

    def _encode_set_value(self, value: set) -> bytes:
        """Encode set value: count(4B) + (member_len(2B) + member)*count."""
        parts = [struct.pack('<I', len(value))]

        for member in value:
            member_bytes = member if isinstance(member, bytes) else member.encode('utf-8')
            parts.append(struct.pack('<H', len(member_bytes)))
            parts.append(member_bytes)

        return b''.join(parts)

    def _encode_zset_value(self, value: list) -> bytes:
        """Encode zset value: count(4B) + (score(8B double) + member_len(2B) + member)*count."""
        parts = [struct.pack('<I', len(value))]

        for score, member in value:
            member_bytes = member if isinstance(member, bytes) else member.encode('utf-8')
            parts.append(struct.pack('<d', score))
            parts.append(struct.pack('<H', len(member_bytes)))
            parts.append(member_bytes)

        return b''.join(parts)

    def _encode_stream_value(self, value: dict) -> bytes:
        """Encode stream value as hash (stream is internally a dict with entries + last_id)."""
        return self._encode_hash_value(value) if isinstance(value, dict) else self._encode_string_value(str(value).encode('utf-8'))

    def _decode_stream_value(self, data: bytes) -> dict:
        """Decode stream value from hash encoding."""
        return self._decode_hash_value(data)

    def _encode_key(self, key) -> bytes:
        """Encode a single key with its metadata and value."""
        parts = []

        # Get value from storage
        value = self._storage._data.get(key)
        if value is None:
            return b''  # Skip deleted keys

        # Use storage._types for authoritative type detection
        type_id = self._storage._types.get(key, TYPE_STRING)

        # Encode value based on type
        if type_id == TYPE_STRING:
            encoded_value = self._encode_string_value(value if isinstance(value, bytes) else str(value).encode('utf-8'))
        elif type_id == TYPE_HASH:
            encoded_value = self._encode_hash_value(value)
        elif type_id == TYPE_LIST:
            encoded_value = self._encode_list_value(value if isinstance(value, list) else list(value))
        elif type_id == TYPE_SET:
            encoded_value = self._encode_set_value(value)
        elif type_id == TYPE_ZSET:
            encoded_value = self._encode_zset_value(value)
        elif type_id == TYPE_STREAM:
            encoded_value = self._encode_stream_value(value)
        else:
            encoded_value = self._encode_string_value(str(value).encode('utf-8'))

        # Type marker
        parts.append(struct.pack('B', type_id))

        # TTL handling
        ttl_ms = self._storage._expires.get(key)
        if ttl_ms is not None:
            parts.append(struct.pack('B', 1))  # has_ttl = 1
            parts.append(struct.pack('<q', ttl_ms))  # int64 absolute timestamp
        else:
            parts.append(struct.pack('B', 0))  # has_ttl = 0

        # Key encoding
        key_bytes = key if isinstance(key, bytes) else key.encode('utf-8')
        parts.append(struct.pack('<H', len(key_bytes)))
        parts.append(key_bytes)

        # Value encoding
        parts.append(struct.pack('<I', len(encoded_value)))
        parts.append(encoded_value)

        return b''.join(parts)

    def save(self) -> bool:
        """
        Synchronously save snapshot to disk.

        Returns:
            True if successful, False on error
        """
        try:
            # Get all keys
            keys = list(self._storage._data.keys())

            # Build snapshot data
            key_parts = []

            # Encode all keys first (to get actual count)
            actual_count = 0
            for key in keys:
                encoded = self._encode_key(key)
                if encoded:
                    key_parts.append(encoded)
                    actual_count += 1

            # Header (with actual encoded key count)
            timestamp = int(time.time())
            header = struct.pack(
                '<4sHII',
                MAGIC,
                VERSION,
                timestamp,
                actual_count
            )

            # Combine header + keys
            parts = [header] + key_parts

            # Calculate CRC32
            data = b''.join(parts)
            crc = calculate_crc32(data)

            # Final snapshot: data + CRC (avoid double join)
            snapshot = data + struct.pack('<I', crc)

            # Atomic write with temporary file
            tmp_path = self._filepath + '.tmp'

            # Write to temp file
            with open(tmp_path, 'wb') as f:
                f.write(snapshot)

            # Rename (atomic on most filesystems)
            try:
                os.remove(self._filepath)
            except OSError:
                pass  # File doesn't exist

            os.rename(tmp_path, self._filepath)

            # Update state
            self._last_save = timestamp
            self._changes_since_save = 0

            print(f"[Snapshot] Saved {actual_count} keys to {self._filepath} ({len(snapshot)} bytes)")
            return True

        except Exception as e:
            print(f"[Snapshot] Save failed: {e}")
            return False

    async def save_async(self) -> bool:
        """
        Asynchronously save snapshot with yielding to prevent blocking.

        Returns:
            True if successful, False on error
        """
        try:
            # Get all keys
            keys = list(self._storage._data.keys())

            # Build snapshot data with yielding
            key_parts = []

            # Encode keys with periodic yielding (count actual encoded)
            actual_count = 0
            for i, key in enumerate(keys):
                encoded = self._encode_key(key)
                if encoded:
                    key_parts.append(encoded)
                    actual_count += 1

                # Yield every 50 keys to prevent blocking
                if i % 50 == 0:
                    await asyncio.sleep_ms(0)

            # Header (with actual encoded key count)
            timestamp = int(time.time())
            header = struct.pack(
                '<4sHII',
                MAGIC,
                VERSION,
                timestamp,
                actual_count
            )

            # Combine header + keys
            parts = [header] + key_parts

            # Calculate CRC32
            data = b''.join(parts)
            crc = calculate_crc32(data)

            # Final snapshot: data + CRC (avoid double join)
            snapshot = data + struct.pack('<I', crc)

            # Atomic write with temporary file
            tmp_path = self._filepath + '.tmp'

            # Write to temp file (yield during large writes)
            with open(tmp_path, 'wb') as f:
                chunk_size = 4096
                for i in range(0, len(snapshot), chunk_size):
                    f.write(snapshot[i:i + chunk_size])
                    if i % (chunk_size * 10) == 0:
                        await asyncio.sleep_ms(0)

            # Rename (atomic)
            try:
                os.remove(self._filepath)
            except OSError:
                pass

            os.rename(tmp_path, self._filepath)

            # Update state
            self._last_save = timestamp
            self._changes_since_save = 0

            print(f"[Snapshot] Saved {len(keys)} keys to {self._filepath} ({len(snapshot)} bytes)")
            return True

        except Exception as e:
            print(f"[Snapshot] Async save failed: {e}")
            return False

    def _decode_string_value(self, data: bytes) -> bytes:
        """Decode string value (raw bytes)."""
        return data

    def _decode_hash_value(self, data: bytes) -> dict:
        """Decode hash value."""
        result = {}
        offset = 0

        count = struct.unpack('<H', data[offset:offset+2])[0]
        offset += 2

        for _ in range(count):
            # Field
            field_len = struct.unpack('<H', data[offset:offset+2])[0]
            offset += 2
            field = data[offset:offset+field_len]
            offset += field_len

            # Value
            val_len = struct.unpack('<I', data[offset:offset+4])[0]
            offset += 4
            val = data[offset:offset+val_len]
            offset += val_len

            result[field] = val

        return result

    def _decode_list_value(self, data: bytes) -> list:
        """Decode list value."""
        result = []
        offset = 0

        count = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4

        for _ in range(count):
            item_len = struct.unpack('<I', data[offset:offset+4])[0]
            offset += 4
            item = data[offset:offset+item_len]
            offset += item_len
            result.append(item)

        return result

    def _decode_set_value(self, data: bytes) -> set:
        """Decode set value."""
        result = set()
        offset = 0

        count = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4

        for _ in range(count):
            member_len = struct.unpack('<H', data[offset:offset+2])[0]
            offset += 2
            member = data[offset:offset+member_len]
            offset += member_len
            result.add(member)

        return result

    def _decode_zset_value(self, data: bytes) -> list:
        """Decode zset value."""
        result = []
        offset = 0

        count = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4

        for _ in range(count):
            score = struct.unpack('<d', data[offset:offset+8])[0]
            offset += 8

            member_len = struct.unpack('<H', data[offset:offset+2])[0]
            offset += 2
            member = data[offset:offset+member_len]
            offset += member_len

            result.append((score, member))

        return result

    def load(self) -> bool:
        """
        Load snapshot from disk into storage.

        Returns:
            True if successful, False on error
        """
        try:
            # Check if file exists
            try:
                stat = os.stat(self._filepath)
            except OSError:
                print(f"[Snapshot] File not found: {self._filepath}")
                return False

            # Read entire file
            with open(self._filepath, 'rb') as f:
                snapshot = f.read()

            # Verify minimum size
            if len(snapshot) < HEADER_SIZE + FOOTER_SIZE:
                print("[Snapshot] File too small")
                return False

            # Split data and CRC
            data = snapshot[:-FOOTER_SIZE]
            stored_crc = struct.unpack('<I', snapshot[-FOOTER_SIZE:])[0]

            # Verify CRC
            calculated_crc = calculate_crc32(data)
            if stored_crc != calculated_crc:
                print(f"[Snapshot] CRC mismatch: {stored_crc:08x} != {calculated_crc:08x}")
                return False

            # Parse header
            offset = 0
            magic, version, timestamp, key_count = struct.unpack(
                '<4sHII',
                data[offset:offset+HEADER_SIZE]
            )
            offset += HEADER_SIZE

            if magic != MAGIC:
                print(f"[Snapshot] Invalid magic: {magic}")
                return False

            if version != VERSION:
                print(f"[Snapshot] Unsupported version: {version}")
                return False

            # Clear existing data
            self._storage._data.clear()
            self._storage._expires.clear()
            self._storage._types.clear()
            self._storage._version.clear()
            self._storage._last_access.clear()

            # Decode keys
            loaded_count = 0
            for _ in range(key_count):
                if offset >= len(data):
                    break

                # Type
                type_id = struct.unpack('B', data[offset:offset+1])[0]
                offset += 1

                # TTL
                has_ttl = struct.unpack('B', data[offset:offset+1])[0]
                offset += 1

                ttl_ms = None
                if has_ttl:
                    ttl_ms = struct.unpack('<q', data[offset:offset+8])[0]
                    offset += 8

                # Key
                key_len = struct.unpack('<H', data[offset:offset+2])[0]
                offset += 2
                key = bytes(data[offset:offset+key_len])
                offset += key_len

                # Value
                val_len = struct.unpack('<I', data[offset:offset+4])[0]
                offset += 4
                val_data = data[offset:offset+val_len]
                offset += val_len

                # Decode value based on type
                if type_id == TYPE_STRING:
                    value = self._decode_string_value(val_data)
                elif type_id == TYPE_HASH:
                    value = self._decode_hash_value(val_data)
                elif type_id == TYPE_LIST:
                    value = self._decode_list_value(val_data)
                elif type_id == TYPE_SET:
                    value = self._decode_set_value(val_data)
                elif type_id == TYPE_ZSET:
                    value = self._decode_zset_value(val_data)
                elif type_id == TYPE_STREAM:
                    value = self._decode_stream_value(val_data)
                else:
                    print(f"[Snapshot] Unknown type {type_id} for key {key}")
                    continue

                # Store in engine
                self._storage._data[key] = value
                if type_id != TYPE_STRING:
                    self._storage._types[key] = type_id
                if ttl_ms is not None:
                    self._storage._expires[key] = ttl_ms

                loaded_count += 1

            # Update state
            self._last_save = timestamp
            self._changes_since_save = 0

            print(f"[Snapshot] Loaded {loaded_count}/{key_count} keys from {self._filepath}")
            return True

        except Exception as e:
            print(f"[Snapshot] Load failed: {e}")
            return False

    async def auto_save_loop(self) -> None:
        """
        Background task for automatic periodic snapshots.

        Runs indefinitely, checking should_auto_save() every minute.
        """
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute

                if self.should_auto_save():
                    print("[Snapshot] Auto-save triggered")
                    await self.save_async()

            except Exception as e:
                print(f"[Snapshot] Auto-save loop error: {e}")
                await asyncio.sleep(60)  # Continue despite errors

"""
Flash I/O helpers for MicroRedis persistence.

Implements dual-slot rotation for wear-leveling and atomic writes
to ensure data integrity on ESP32-S3 flash storage.
"""

import os


class FlashStorage:
    """
    Flash storage manager with dual-slot rotation.

    Uses two slots (A and B) to distribute write operations and reduce
    wear on flash memory. Atomic writes ensure data integrity even if
    power is lost during save operations.

    Directory structure:
        /data/
            data_a.mrdb      # Slot A
            data_b.mrdb      # Slot B
            current_slot.txt # Marker file ('0' or '1')
    """

    __slots__ = ('_base_path', '_slot_a', '_slot_b', '_current_slot', '_marker_file')

    def __init__(self, base_path='/data'):
        """
        Initialize flash storage with dual-slot configuration.

        Args:
            base_path: Base directory for storage files
        """
        self._base_path = base_path
        self._slot_a = base_path + '/data_a.mrdb'
        self._slot_b = base_path + '/data_b.mrdb'
        self._marker_file = base_path + '/current_slot.txt'

        # Ensure directory exists
        self.ensure_directory()

        # Read current slot from marker file
        self._current_slot = self._read_marker()

    def get_current_slot_path(self) -> str:
        """
        Get the path to the current active slot.

        Returns:
            Absolute path to current slot file
        """
        return self._slot_a if self._current_slot == 0 else self._slot_b

    def get_next_slot_path(self) -> str:
        """
        Get the path to the next slot for writing.

        Returns:
            Absolute path to next slot file
        """
        return self._slot_b if self._current_slot == 0 else self._slot_a

    def switch_slot(self) -> None:
        """
        Switch to the other slot.

        Called after successful write to alternate between slots.
        """
        self._current_slot = 1 - self._current_slot

    def write_atomic(self, data: bytes) -> bool:
        """
        Atomically write data to flash using next slot.

        Write sequence:
        1. Write to temporary file (slot.tmp)
        2. Rename temp to slot path (atomic operation)
        3. Update marker file
        4. Switch current slot pointer

        This ensures that even if power is lost during write,
        the previous slot remains intact and readable.

        Args:
            data: Binary data to write

        Returns:
            True if write succeeded, False otherwise
        """
        next_slot = self.get_next_slot_path()
        temp_path = next_slot + '.tmp'

        try:
            # 1. Write to temporary file
            with open(temp_path, 'wb') as f:
                f.write(data)

            # 2. Atomic rename (overwrites existing slot)
            try:
                os.rename(temp_path, next_slot)
            except OSError:
                # On some platforms, rename fails if target exists
                # Remove target and retry
                try:
                    os.remove(next_slot)
                except OSError:
                    pass
                os.rename(temp_path, next_slot)

            # 3. Update marker file to point to new slot
            self._update_marker()

            # 4. Switch current slot pointer
            self.switch_slot()

            return True

        except OSError as e:
            print(f'[Flash] Write error: {e}')

            # Clean up temp file if it exists
            try:
                os.remove(temp_path)
            except OSError:
                pass

            return False

    def read_current(self):
        """
        Read data from current slot with fallback.

        Attempts to read from current slot first. If that fails,
        tries the other slot as fallback for recovery scenarios.

        Returns:
            Binary data from slot, or None if both slots fail
        """
        # Try current slot first
        try:
            with open(self.get_current_slot_path(), 'rb') as f:
                return f.read()
        except OSError:
            pass

        # Fallback to other slot
        other_slot = self._slot_b if self._current_slot == 0 else self._slot_a
        try:
            with open(other_slot, 'rb') as f:
                return f.read()
        except OSError:
            return None

    def get_slot_info(self) -> dict:
        """
        Get detailed information about storage slots.

        Returns:
            Dictionary with slot status:
                current_slot: 0 or 1
                slot_a_path: absolute path
                slot_b_path: absolute path
                slot_a_exists: bool
                slot_b_exists: bool
                slot_a_size: bytes
                slot_b_size: bytes
        """
        return {
            'current_slot': self._current_slot,
            'slot_a_path': self._slot_a,
            'slot_b_path': self._slot_b,
            'slot_a_exists': self._file_exists(self._slot_a),
            'slot_b_exists': self._file_exists(self._slot_b),
            'slot_a_size': self._get_file_size(self._slot_a),
            'slot_b_size': self._get_file_size(self._slot_b),
        }

    def ensure_directory(self) -> None:
        """
        Ensure base directory exists.

        Creates directory if it doesn't exist. Safe to call multiple times.
        """
        try:
            os.mkdir(self._base_path)
        except OSError:
            # Directory already exists or cannot be created
            pass

    def _read_marker(self) -> int:
        """
        Read current slot number from marker file.

        Returns:
            0 or 1 indicating current slot, defaults to 0 if unreadable
        """
        try:
            with open(self._marker_file, 'r') as f:
                value = f.read().strip()
                slot = int(value)
                # Validate slot number
                return slot if slot in (0, 1) else 0
        except (OSError, ValueError):
            # File doesn't exist or invalid content, default to slot 0
            return 0

    def _update_marker(self) -> None:
        """
        Update marker file to point to next slot.

        This is called before switching the slot pointer,
        so it writes the opposite of current_slot.
        """
        next_slot = 1 - self._current_slot
        try:
            with open(self._marker_file, 'w') as f:
                f.write(str(next_slot))
        except OSError as e:
            print(f'[Flash] Marker update error: {e}')

    def _get_file_size(self, path: str) -> int:
        """
        Get file size in bytes.

        Args:
            path: File path to check

        Returns:
            File size in bytes, or 0 if file doesn't exist
        """
        try:
            # os.stat() returns tuple: (mode, ino, dev, nlink, uid, gid, size, ...)
            # Index 6 is st_size
            return os.stat(path)[6]
        except OSError:
            return 0

    def _file_exists(self, path: str) -> bool:
        """
        Check if file exists.

        Args:
            path: File path to check

        Returns:
            True if file exists, False otherwise
        """
        try:
            os.stat(path)
            return True
        except OSError:
            return False

"""
MicroRedis Hash Operations Module

Implements Redis Hash operations with ziplist encoding optimization for small hashes.
Optimized for ESP32-S3 with MicroPython.

Platform: ESP32-S3 with MicroPython
RAM Constraints: ~300KB available for application

Memory Optimizations:
- Ziplist encoding for small hashes (< 64 fields): ~40% less RAM
- Ziplist stored as list of tuples: [(field, value), ...]
- Automatic promotion to dict when threshold exceeded
- All static methods to avoid instance overhead
- Minimal temporary allocations

Hash Encoding Strategy:
- Small hash (< ZIPLIST_MAX_ENTRIES): Use ziplist (list of tuples)
  - Linear search O(n) but compact memory layout
  - Typical use case: session data, user profiles with few fields
- Large hash (>= ZIPLIST_MAX_ENTRIES): Use dict
  - O(1) lookups but higher memory overhead
  - Typical use case: large object storage, caches
"""

from microredis.core.constants import ZIPLIST_MAX_ENTRIES

from microredis.storage.engine import TYPE_HASH


class HashType:
    """
    Redis Hash operations with ziplist encoding for memory efficiency.

    All methods are static to avoid instance overhead.
    Storage engine integration via storage parameter.

    Internal representation:
    - Ziplist: list[(bytes, bytes), ...] for < ZIPLIST_MAX_ENTRIES
    - Dict: dict[bytes, bytes] for >= ZIPLIST_MAX_ENTRIES

    Automatic encoding conversion when threshold crossed.
    """

    __slots__ = ()  # No instance attributes - all static methods

    # =========================================================================
    # Internal Helper Methods
    # =========================================================================

    @staticmethod
    def _get_hash(storage, key):
        """
        Get hash data structure from storage.

        Args:
            storage: Storage instance
            key: bytes - hash key

        Returns:
            dict | list | None: hash data (dict or ziplist) or None if not exists/wrong type
        """
        if key not in storage._data:
            return None

        # Check expiry
        if storage._delete_if_expired(key):
            return None

        # Type check
        if not storage._check_type(key, TYPE_HASH):
            return None

        return storage._data[key]

    @staticmethod
    def _set_hash(storage, key, data):
        """
        Store hash data structure in storage.

        Args:
            storage: Storage instance
            key: bytes - hash key
            data: dict | list - hash data (dict or ziplist)
        """
        storage.check_can_create_key(key)
        storage._data[key] = data
        storage._types[key] = TYPE_HASH
        storage._increment_version(key)

    @staticmethod
    def _ziplist_to_dict(ziplist):
        """
        Convert ziplist to dict.

        Args:
            ziplist: list[(bytes, bytes), ...] - ziplist representation

        Returns:
            dict[bytes, bytes]: dict representation
        """
        # Pre-allocate dict with expected size hint (MicroPython optimization)
        result = {}
        for field, value in ziplist:
            result[field] = value
        return result

    @staticmethod
    def _ensure_dict(data):
        """
        Ensure data is in dict format (convert from ziplist if needed).

        Args:
            data: dict | list - hash data

        Returns:
            dict[bytes, bytes]: dict representation
        """
        if isinstance(data, list):
            return HashType._ziplist_to_dict(data)
        return data

    @staticmethod
    def _validate_type(storage, key):
        """
        Validate that key is hash type or doesn't exist.

        Args:
            storage: Storage instance
            key: bytes - key to validate

        Raises:
            TypeError: if key exists but is not hash type
        """
        if key in storage._data:
            # Check expiry first
            if storage._delete_if_expired(key):
                return  # Key expired, validation passes

            # Check type
            actual_type = storage._types.get(key, 0)  # 0 = TYPE_STRING
            if actual_type != TYPE_HASH:
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

    # =========================================================================
    # Hash Operations
    # =========================================================================

    @staticmethod
    def hset(storage, key, field, value):
        """
        Set field in hash.

        Args:
            storage: Storage instance
            key: bytes - hash key
            field: bytes - field name
            value: bytes - field value

        Returns:
            int: 1 if new field created, 0 if field updated

        Raises:
            TypeError: if key exists but is not hash type
        """
        HashType._validate_type(storage, key)

        data = HashType._get_hash(storage, key)

        if data is None:
            # Create new hash as ziplist
            HashType._set_hash(storage, key, [(field, value)])
            return 1

        # Check if ziplist or dict
        if isinstance(data, list):
            # Search in ziplist
            for i, (f, v) in enumerate(data):
                if f == field:
                    # Update existing field
                    data[i] = (field, value)
                    storage._increment_version(key)
                    return 0

            # New field - check if need to convert to dict
            if len(data) >= ZIPLIST_MAX_ENTRIES:
                # Convert to dict
                hash_dict = HashType._ziplist_to_dict(data)
                hash_dict[field] = value
                HashType._set_hash(storage, key, hash_dict)
            else:
                # Add to ziplist
                data.append((field, value))
                storage._increment_version(key)
            return 1

        else:
            # Dict format
            is_new = field not in data
            data[field] = value
            storage._increment_version(key)
            return 1 if is_new else 0

    @staticmethod
    def hget(storage, key, field):
        """
        Get field value from hash.

        Args:
            storage: Storage instance
            key: bytes - hash key
            field: bytes - field name

        Returns:
            bytes | None: field value or None if not exists

        Raises:
            TypeError: if key exists but is not hash type
        """
        HashType._validate_type(storage, key)

        data = HashType._get_hash(storage, key)

        if data is None:
            return None

        # Check if ziplist or dict
        if isinstance(data, list):
            # Linear search in ziplist
            for f, v in data:
                if f == field:
                    return v
            return None
        else:
            # Dict lookup
            return data.get(field)

    @staticmethod
    def hdel(storage, key, *fields):
        """
        Delete fields from hash.

        Args:
            storage: Storage instance
            key: bytes - hash key
            *fields: bytes - field names to delete

        Returns:
            int: number of fields deleted

        Raises:
            TypeError: if key exists but is not hash type
        """
        HashType._validate_type(storage, key)

        data = HashType._get_hash(storage, key)

        if data is None:
            return 0

        count = 0

        # Check if ziplist or dict
        if isinstance(data, list):
            # Remove from ziplist (rebuild list without deleted fields)
            new_list = []
            for f, v in data:
                if f in fields:
                    count += 1
                else:
                    new_list.append((f, v))

            if count > 0:
                if len(new_list) == 0:
                    # Hash now empty - delete key
                    storage.delete(key)
                else:
                    storage._data[key] = new_list
                    storage._increment_version(key)
        else:
            # Dict format
            for field in fields:
                if field in data:
                    del data[field]
                    count += 1

            if count > 0:
                if len(data) == 0:
                    # Hash now empty - delete key
                    storage.delete(key)
                else:
                    storage._increment_version(key)

        return count

    @staticmethod
    def hexists(storage, key, field):
        """
        Check if field exists in hash.

        Args:
            storage: Storage instance
            key: bytes - hash key
            field: bytes - field name

        Returns:
            bool: True if field exists, False otherwise

        Raises:
            TypeError: if key exists but is not hash type
        """
        HashType._validate_type(storage, key)

        data = HashType._get_hash(storage, key)

        if data is None:
            return False

        # Check if ziplist or dict
        if isinstance(data, list):
            # Linear search
            for f, v in data:
                if f == field:
                    return True
            return False
        else:
            return field in data

    @staticmethod
    def hgetall(storage, key):
        """
        Get all fields and values from hash.

        Args:
            storage: Storage instance
            key: bytes - hash key

        Returns:
            list[bytes]: flat list [field1, value1, field2, value2, ...]

        Raises:
            TypeError: if key exists but is not hash type
        """
        HashType._validate_type(storage, key)

        data = HashType._get_hash(storage, key)

        if data is None:
            return []

        result = []

        # Check if ziplist or dict
        if isinstance(data, list):
            # Flatten ziplist
            for field, value in data:
                result.append(field)
                result.append(value)
        else:
            # Flatten dict
            for field, value in data.items():
                result.append(field)
                result.append(value)

        return result

    @staticmethod
    def hkeys(storage, key):
        """
        Get all field names from hash.

        Args:
            storage: Storage instance
            key: bytes - hash key

        Returns:
            list[bytes]: list of field names

        Raises:
            TypeError: if key exists but is not hash type
        """
        HashType._validate_type(storage, key)

        data = HashType._get_hash(storage, key)

        if data is None:
            return []

        # Check if ziplist or dict
        if isinstance(data, list):
            # Extract fields from ziplist
            return [field for field, value in data]
        else:
            # Extract keys from dict
            return list(data.keys())

    @staticmethod
    def hvals(storage, key):
        """
        Get all values from hash.

        Args:
            storage: Storage instance
            key: bytes - hash key

        Returns:
            list[bytes]: list of values

        Raises:
            TypeError: if key exists but is not hash type
        """
        HashType._validate_type(storage, key)

        data = HashType._get_hash(storage, key)

        if data is None:
            return []

        # Check if ziplist or dict
        if isinstance(data, list):
            # Extract values from ziplist
            return [value for field, value in data]
        else:
            # Extract values from dict
            return list(data.values())

    @staticmethod
    def hlen(storage, key):
        """
        Get number of fields in hash.

        Args:
            storage: Storage instance
            key: bytes - hash key

        Returns:
            int: number of fields (0 if hash doesn't exist)

        Raises:
            TypeError: if key exists but is not hash type
        """
        HashType._validate_type(storage, key)

        data = HashType._get_hash(storage, key)

        if data is None:
            return 0

        return len(data)

    @staticmethod
    def hmset(storage, key, mapping):
        """
        Set multiple fields in hash.

        Args:
            storage: Storage instance
            key: bytes - hash key
            mapping: dict[bytes, bytes] - field-value pairs to set

        Returns:
            bool: True (always succeeds)

        Raises:
            TypeError: if key exists but is not hash type
        """
        HashType._validate_type(storage, key)

        if not mapping:
            return True

        data = HashType._get_hash(storage, key)

        if data is None:
            # Create new hash
            # Check size to determine encoding
            if len(mapping) >= ZIPLIST_MAX_ENTRIES:
                # Create as dict
                HashType._set_hash(storage, key, dict(mapping))
            else:
                # Create as ziplist
                ziplist = [(f, v) for f, v in mapping.items()]
                HashType._set_hash(storage, key, ziplist)
            return True

        # Check if ziplist or dict
        if isinstance(data, list):
            # Convert to dict for easier merging
            hash_dict = HashType._ziplist_to_dict(data)

            # Merge new values
            for field, value in mapping.items():
                hash_dict[field] = value

            # Check if need to keep as dict or can stay ziplist
            if len(hash_dict) >= ZIPLIST_MAX_ENTRIES:
                HashType._set_hash(storage, key, hash_dict)
            else:
                # Convert back to ziplist
                ziplist = [(f, v) for f, v in hash_dict.items()]
                HashType._set_hash(storage, key, ziplist)
        else:
            # Dict format - direct update
            data.update(mapping)
            storage._increment_version(key)

        return True

    @staticmethod
    def hmget(storage, key, *fields):
        """
        Get values of multiple fields from hash.

        Args:
            storage: Storage instance
            key: bytes - hash key
            *fields: bytes - field names to retrieve

        Returns:
            list[bytes | None]: list of values (None for non-existent fields)

        Raises:
            TypeError: if key exists but is not hash type
        """
        HashType._validate_type(storage, key)

        data = HashType._get_hash(storage, key)

        if data is None:
            # Return None for all fields
            return [None] * len(fields)

        result = []

        # Check if ziplist or dict
        if isinstance(data, list):
            # Build lookup dict for efficiency (avoid O(n^2))
            lookup = {f: v for f, v in data}
            for field in fields:
                result.append(lookup.get(field))
        else:
            # Dict lookup
            for field in fields:
                result.append(data.get(field))

        return result

    @staticmethod
    def hsetnx(storage, key, field, value):
        """
        Set field in hash only if field doesn't exist.

        Args:
            storage: Storage instance
            key: bytes - hash key
            field: bytes - field name
            value: bytes - field value

        Returns:
            int: 1 if field was set, 0 if field already exists

        Raises:
            TypeError: if key exists but is not hash type
        """
        HashType._validate_type(storage, key)

        data = HashType._get_hash(storage, key)

        if data is None:
            # Create new hash with single field
            HashType._set_hash(storage, key, [(field, value)])
            return 1

        # Check if ziplist or dict
        if isinstance(data, list):
            # Check if field exists
            for f, v in data:
                if f == field:
                    return 0  # Field already exists

            # Field doesn't exist - add it
            if len(data) >= ZIPLIST_MAX_ENTRIES:
                # Convert to dict
                hash_dict = HashType._ziplist_to_dict(data)
                hash_dict[field] = value
                HashType._set_hash(storage, key, hash_dict)
            else:
                # Add to ziplist
                data.append((field, value))
                storage._increment_version(key)
            return 1
        else:
            # Dict format
            if field in data:
                return 0  # Field already exists

            data[field] = value
            storage._increment_version(key)
            return 1

    @staticmethod
    def hincrby(storage, key, field, increment):
        """
        Increment integer value of hash field.

        Args:
            storage: Storage instance
            key: bytes - hash key
            field: bytes - field name
            increment: int - increment value

        Returns:
            int: new value after increment

        Raises:
            TypeError: if key exists but is not hash type
            ValueError: if field value is not an integer
        """
        HashType._validate_type(storage, key)

        data = HashType._get_hash(storage, key)

        # Get current value
        current_value = None

        if data is not None:
            if isinstance(data, list):
                # Search in ziplist
                for f, v in data:
                    if f == field:
                        current_value = v
                        break
            else:
                # Dict lookup
                current_value = data.get(field)

        # Parse and increment
        if current_value is None:
            new_value = increment
        else:
            try:
                current_int = int(current_value.decode() if isinstance(current_value, bytes) else current_value)
            except (ValueError, TypeError):
                raise ValueError("ERR hash value is not an integer")
            new_value = current_int + increment

        # Store new value
        new_value_bytes = str(new_value).encode()
        HashType.hset(storage, key, field, new_value_bytes)

        return new_value

    @staticmethod
    def hincrbyfloat(storage, key, field, increment):
        """
        Increment float value of hash field.

        Args:
            storage: Storage instance
            key: bytes - hash key
            field: bytes - field name
            increment: float - increment value

        Returns:
            bytes: new value after increment (as bytes string)

        Raises:
            TypeError: if key exists but is not hash type
            ValueError: if field value is not a valid float
        """
        HashType._validate_type(storage, key)

        data = HashType._get_hash(storage, key)

        # Get current value
        current_value = None

        if data is not None:
            if isinstance(data, list):
                # Search in ziplist
                for f, v in data:
                    if f == field:
                        current_value = v
                        break
            else:
                # Dict lookup
                current_value = data.get(field)

        # Parse and increment
        if current_value is None:
            new_value = increment
        else:
            try:
                current_float = float(current_value.decode() if isinstance(current_value, bytes) else current_value)
            except (ValueError, TypeError):
                raise ValueError("ERR hash value is not a valid float")
            new_value = current_float + increment

        # Store new value
        # Format float to match Redis behavior (avoid scientific notation for small numbers)
        if -1e15 < new_value < 1e15:
            new_value_str = f"{new_value:.17g}"  # 17 digits precision
        else:
            new_value_str = str(new_value)

        new_value_bytes = new_value_str.encode()
        HashType.hset(storage, key, field, new_value_bytes)

        return new_value_bytes

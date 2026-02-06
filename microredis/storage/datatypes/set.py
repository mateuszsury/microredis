"""
MicroRedis Set Operations Module

Implements Redis Set operations with intset encoding optimization for small integer sets.
Optimized for ESP32-S3 with MicroPython.

Platform: ESP32-S3 with MicroPython
RAM Constraints: ~300KB available for application

Memory Optimizations:
- Intset encoding for small sets containing only integers (< 512 elements): ~60% less RAM
- Intset stored as sorted list of ints for binary search lookups
- Automatic promotion to set when: non-integer added or threshold exceeded
- All static methods to avoid instance overhead
- Minimal temporary allocations

Set Encoding Strategy:
- Small integer set (< INTSET_MAX_ENTRIES and all integers): Use intset (sorted list[int])
  - Binary search O(log n) for membership checks
  - Sorted insertion maintains order
  - Typical use case: user IDs, timestamps, counters
- Large or mixed set (>= INTSET_MAX_ENTRIES or has non-integers): Use set[bytes]
  - O(1) lookups but higher memory overhead
  - Typical use case: tags, unique strings, mixed data
"""

from microredis.core.constants import INTSET_MAX_ENTRIES
import random

from microredis.storage.engine import TYPE_SET


# Binary search helpers (optimized for MicroPython)
def _bisect_left(a, x):
    """
    Binary search: find leftmost position to insert x in sorted list a.

    Args:
        a: list[int] - sorted list
        x: int - value to find

    Returns:
        int: insertion position
    """
    lo, hi = 0, len(a)
    while lo < hi:
        mid = (lo + hi) // 2
        if a[mid] < x:
            lo = mid + 1
        else:
            hi = mid
    return lo


def _bisect_right(a, x):
    """
    Binary search: find rightmost position to insert x in sorted list a.

    Args:
        a: list[int] - sorted list
        x: int - value to find

    Returns:
        int: insertion position
    """
    lo, hi = 0, len(a)
    while lo < hi:
        mid = (lo + hi) // 2
        if x < a[mid]:
            hi = mid
        else:
            lo = mid + 1
    return lo


def _insort_left(a, x):
    """
    Insert x in sorted list a, maintaining sort order.

    Args:
        a: list[int] - sorted list (modified in place)
        x: int - value to insert
    """
    pos = _bisect_left(a, x)
    a.insert(pos, x)


class SetOperations:
    """
    Redis Set operations with intset encoding for memory efficiency.

    All methods are static to avoid instance overhead.
    Storage engine integration via storage parameter.

    Internal representation:
    - Intset: list[int] (sorted) for small sets with only integers
    - Regular set: set[bytes] for large sets or sets with non-integers

    Automatic encoding conversion when:
    - Non-integer member added to intset -> convert to set[bytes]
    - Intset size exceeds INTSET_MAX_ENTRIES -> convert to set[bytes]
    """

    __slots__ = ()  # No instance attributes - all static methods

    # =========================================================================
    # Internal Helper Methods
    # =========================================================================

    @staticmethod
    def _get_set(storage, key):
        """
        Get set data structure from storage.

        Args:
            storage: Storage instance
            key: bytes - set key

        Returns:
            set[bytes] | list[int] | None: set data (set or intset) or None if not exists/wrong type
        """
        if key not in storage._data:
            return None

        # Check expiry
        if storage._delete_if_expired(key):
            return None

        # Type check
        if not storage._check_type(key, TYPE_SET):
            return None

        return storage._data[key]

    @staticmethod
    def _set_set(storage, key, data):
        """
        Store set data structure in storage.

        Args:
            storage: Storage instance
            key: bytes - set key
            data: set[bytes] | list[int] - set data (set or intset)
        """
        storage.check_can_create_key(key)
        storage._data[key] = data
        storage._types[key] = TYPE_SET
        storage._increment_version(key)

    @staticmethod
    def _is_intset(data):
        """
        Check if data is an intset (sorted list of ints).

        Args:
            data: set | list - set data

        Returns:
            bool: True if intset, False if regular set
        """
        return isinstance(data, list)

    @staticmethod
    def _try_convert_to_int(value):
        """
        Try to convert bytes value to integer.

        Args:
            value: bytes - value to convert

        Returns:
            int | None: integer value or None if not parseable
        """
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _convert_intset_to_set(intset):
        """
        Convert intset to regular set[bytes].

        Args:
            intset: list[int] - intset representation

        Returns:
            set[bytes]: regular set representation
        """
        return {str(i).encode() for i in intset}

    @staticmethod
    def _intset_add(intset, value):
        """
        Add integer to intset (binary insert, maintaining sort order).

        Args:
            intset: list[int] - sorted list (modified in place)
            value: int - value to add

        Returns:
            bool: True if value was added, False if already exists
        """
        pos = _bisect_left(intset, value)

        # Check if already exists
        if pos < len(intset) and intset[pos] == value:
            return False

        # Insert at correct position
        intset.insert(pos, value)
        return True

    @staticmethod
    def _intset_remove(intset, value):
        """
        Remove integer from intset (binary search).

        Args:
            intset: list[int] - sorted list (modified in place)
            value: int - value to remove

        Returns:
            bool: True if value was removed, False if not found
        """
        pos = _bisect_left(intset, value)

        # Check if exists
        if pos < len(intset) and intset[pos] == value:
            del intset[pos]
            return True

        return False

    @staticmethod
    def _intset_contains(intset, value):
        """
        Check if integer exists in intset (binary search).

        Args:
            intset: list[int] - sorted list
            value: int - value to find

        Returns:
            bool: True if value exists, False otherwise
        """
        pos = _bisect_left(intset, value)
        return pos < len(intset) and intset[pos] == value

    @staticmethod
    def _validate_type(storage, key):
        """
        Validate that key is set type or doesn't exist.

        Args:
            storage: Storage instance
            key: bytes - key to validate

        Raises:
            TypeError: if key exists but is not set type
        """
        if key in storage._data:
            # Check expiry first
            if storage._delete_if_expired(key):
                return  # Key expired, validation passes

            # Check type
            actual_type = storage._types.get(key, 0)  # 0 = TYPE_STRING
            if actual_type != TYPE_SET:
                raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

    # =========================================================================
    # Basic Set Operations
    # =========================================================================

    @staticmethod
    def sadd(storage, key, *members):
        """
        Add members to set.

        Args:
            storage: Storage instance
            key: bytes - set key
            *members: bytes - members to add

        Returns:
            int: number of members actually added (excludes already existing)

        Raises:
            TypeError: if key exists but is not set type
        """
        SetOperations._validate_type(storage, key)

        data = SetOperations._get_set(storage, key)

        if data is None:
            # Create new set - try intset first
            intset = []
            regular_set = set()
            use_intset = True

            for member in members:
                if use_intset and len(intset) < INTSET_MAX_ENTRIES:
                    int_val = SetOperations._try_convert_to_int(member)
                    if int_val is not None:
                        if int_val not in intset:
                            _insort_left(intset, int_val)
                    else:
                        # Can't use intset anymore - convert what we have
                        use_intset = False
                        regular_set = SetOperations._convert_intset_to_set(intset)
                        regular_set.add(member)
                else:
                    # Too large for intset or already switched
                    if use_intset:
                        use_intset = False
                        regular_set = SetOperations._convert_intset_to_set(intset)
                    regular_set.add(member)

            if use_intset:
                SetOperations._set_set(storage, key, intset)
                return len(intset)
            else:
                SetOperations._set_set(storage, key, regular_set)
                return len(regular_set)

        # Set exists - add to it
        if SetOperations._is_intset(data):
            # Intset format
            count = 0
            converted = False

            for member in members:
                if converted:
                    # Already converted to regular set
                    if member not in data:
                        data.add(member)
                        count += 1
                    continue

                # Try to add as integer
                int_val = SetOperations._try_convert_to_int(member)

                if int_val is not None:
                    # Can add as integer
                    if len(data) >= INTSET_MAX_ENTRIES:
                        # Convert to regular set
                        data = SetOperations._convert_intset_to_set(data)
                        data.add(member)
                        SetOperations._set_set(storage, key, data)
                        count += 1
                        converted = True
                    else:
                        # Add to intset
                        if SetOperations._intset_add(data, int_val):
                            count += 1
                else:
                    # Non-integer - must convert to regular set
                    data = SetOperations._convert_intset_to_set(data)
                    data.add(member)
                    SetOperations._set_set(storage, key, data)
                    count += 1
                    converted = True

            if not converted:
                storage._increment_version(key)

            return count
        else:
            # Regular set format
            count = 0
            for member in members:
                if member not in data:
                    data.add(member)
                    count += 1

            if count > 0:
                storage._increment_version(key)

            return count

    @staticmethod
    def srem(storage, key, *members):
        """
        Remove members from set.

        Args:
            storage: Storage instance
            key: bytes - set key
            *members: bytes - members to remove

        Returns:
            int: number of members actually removed

        Raises:
            TypeError: if key exists but is not set type
        """
        SetOperations._validate_type(storage, key)

        data = SetOperations._get_set(storage, key)

        if data is None:
            return 0

        count = 0

        if SetOperations._is_intset(data):
            # Intset format
            for member in members:
                int_val = SetOperations._try_convert_to_int(member)
                if int_val is not None:
                    if SetOperations._intset_remove(data, int_val):
                        count += 1
        else:
            # Regular set format
            for member in members:
                if member in data:
                    data.remove(member)
                    count += 1

        if count > 0:
            # Check if set is now empty
            if len(data) == 0:
                storage.delete(key)
            else:
                storage._increment_version(key)

        return count

    @staticmethod
    def sismember(storage, key, member):
        """
        Check if member is in set.

        Args:
            storage: Storage instance
            key: bytes - set key
            member: bytes - member to check

        Returns:
            bool: True if member exists, False otherwise

        Raises:
            TypeError: if key exists but is not set type
        """
        SetOperations._validate_type(storage, key)

        data = SetOperations._get_set(storage, key)

        if data is None:
            return False

        if SetOperations._is_intset(data):
            # Intset format - binary search
            int_val = SetOperations._try_convert_to_int(member)
            if int_val is not None:
                return SetOperations._intset_contains(data, int_val)
            return False
        else:
            # Regular set format
            return member in data

    @staticmethod
    def smembers(storage, key):
        """
        Get all members of set.

        Args:
            storage: Storage instance
            key: bytes - set key

        Returns:
            set[bytes]: set of all members (empty set if key doesn't exist)

        Raises:
            TypeError: if key exists but is not set type
        """
        SetOperations._validate_type(storage, key)

        data = SetOperations._get_set(storage, key)

        if data is None:
            return set()

        if SetOperations._is_intset(data):
            # Convert intset to set[bytes]
            return SetOperations._convert_intset_to_set(data)
        else:
            # Return copy of regular set
            return set(data)

    @staticmethod
    def scard(storage, key):
        """
        Get cardinality (size) of set.

        Args:
            storage: Storage instance
            key: bytes - set key

        Returns:
            int: number of members in set (0 if key doesn't exist)

        Raises:
            TypeError: if key exists but is not set type
        """
        SetOperations._validate_type(storage, key)

        data = SetOperations._get_set(storage, key)

        if data is None:
            return 0

        return len(data)

    @staticmethod
    def spop(storage, key, count=1):
        """
        Remove and return random members from set.

        Args:
            storage: Storage instance
            key: bytes - set key
            count: int - number of members to pop (default 1)

        Returns:
            bytes | list[bytes] | None: single member if count=1, list if count>1, None if set empty

        Raises:
            TypeError: if key exists but is not set type
        """
        SetOperations._validate_type(storage, key)

        data = SetOperations._get_set(storage, key)

        if data is None:
            return None if count == 1 else []

        if len(data) == 0:
            return None if count == 1 else []

        # Limit count to set size
        count = min(count, len(data))

        if SetOperations._is_intset(data):
            # Intset format - randomly select and remove
            result = []
            for _ in range(count):
                if len(data) == 0:
                    break
                idx = random.randrange(len(data))
                value = data.pop(idx)
                result.append(str(value).encode())
        else:
            # Regular set format
            result = []
            for _ in range(count):
                if len(data) == 0:
                    break
                member = data.pop()
                result.append(member)

        # Check if set is now empty
        if len(data) == 0:
            storage.delete(key)
        else:
            storage._increment_version(key)

        # Return format based on count
        if count == 1:
            return result[0] if result else None
        else:
            return result

    @staticmethod
    def srandmember(storage, key, count=None):
        """
        Get random members from set without removing.

        Args:
            storage: Storage instance
            key: bytes - set key
            count: int | None - number of members to return (None = 1)

        Returns:
            bytes | list[bytes] | None: single member if count=None, list otherwise

        Raises:
            TypeError: if key exists but is not set type
        """
        SetOperations._validate_type(storage, key)

        data = SetOperations._get_set(storage, key)

        if data is None:
            return None if count is None else []

        if len(data) == 0:
            return None if count is None else []

        # Convert to list for random access
        if SetOperations._is_intset(data):
            members = [str(i).encode() for i in data]
        else:
            members = list(data)

        if count is None:
            # Return single random member
            return random.choice(members)

        # Return multiple members
        if count > 0:
            # Return unique members (no duplicates)
            count = min(count, len(members))
            return random.sample(members, count)
        else:
            # Negative count - allow duplicates
            count = abs(count)
            return [random.choice(members) for _ in range(count)]

    @staticmethod
    def smove(storage, source, dest, member):
        """
        Move member from source set to destination set.

        Args:
            storage: Storage instance
            source: bytes - source set key
            dest: bytes - destination set key
            member: bytes - member to move

        Returns:
            bool: True if member was moved, False if member doesn't exist in source

        Raises:
            TypeError: if source or dest exists but is not set type
        """
        SetOperations._validate_type(storage, source)
        SetOperations._validate_type(storage, dest)

        source_data = SetOperations._get_set(storage, source)

        if source_data is None:
            return False

        # Check if member exists in source
        removed = False

        if SetOperations._is_intset(source_data):
            int_val = SetOperations._try_convert_to_int(member)
            if int_val is not None:
                removed = SetOperations._intset_remove(source_data, int_val)
        else:
            if member in source_data:
                source_data.remove(member)
                removed = True

        if not removed:
            return False

        # Remove from source
        if len(source_data) == 0:
            storage.delete(source)
        else:
            storage._increment_version(source)

        # Add to destination
        SetOperations.sadd(storage, dest, member)

        return True

    # =========================================================================
    # Set Operations - Intersection, Union, Difference
    # =========================================================================

    @staticmethod
    def sinter(storage, *keys):
        """
        Get intersection of multiple sets.

        Args:
            storage: Storage instance
            *keys: bytes - set keys to intersect

        Returns:
            set[bytes]: intersection of all sets

        Raises:
            TypeError: if any key exists but is not set type
        """
        if not keys:
            return set()

        # Validate all keys
        for key in keys:
            SetOperations._validate_type(storage, key)

        # Get first set
        result = SetOperations.smembers(storage, keys[0])

        if not result:
            return set()

        # Intersect with remaining sets
        for key in keys[1:]:
            members = SetOperations.smembers(storage, key)
            result &= members  # Intersection

            if not result:
                return set()  # Early exit if intersection becomes empty

        return result

    @staticmethod
    def sinterstore(storage, dest, *keys):
        """
        Store intersection of multiple sets in destination key.

        Args:
            storage: Storage instance
            dest: bytes - destination key
            *keys: bytes - set keys to intersect

        Returns:
            int: number of members in resulting set

        Raises:
            TypeError: if any key exists but is not set type
        """
        result = SetOperations.sinter(storage, *keys)

        # Delete destination key first
        storage.delete(dest)

        if result:
            storage.check_can_create_key(dest)
            storage._data[dest] = result
            storage._types[dest] = TYPE_SET
            storage._increment_version(dest)

        return len(result)

    @staticmethod
    def sunion(storage, *keys):
        """
        Get union of multiple sets.

        Args:
            storage: Storage instance
            *keys: bytes - set keys to union

        Returns:
            set[bytes]: union of all sets

        Raises:
            TypeError: if any key exists but is not set type
        """
        if not keys:
            return set()

        # Validate all keys
        for key in keys:
            SetOperations._validate_type(storage, key)

        # Union all sets
        result = set()
        for key in keys:
            members = SetOperations.smembers(storage, key)
            result |= members  # Union

        return result

    @staticmethod
    def sunionstore(storage, dest, *keys):
        """
        Store union of multiple sets in destination key.

        Args:
            storage: Storage instance
            dest: bytes - destination key
            *keys: bytes - set keys to union

        Returns:
            int: number of members in resulting set

        Raises:
            TypeError: if any key exists but is not set type
        """
        result = SetOperations.sunion(storage, *keys)

        # Delete destination key first
        storage.delete(dest)

        if result:
            storage.check_can_create_key(dest)
            storage._data[dest] = result
            storage._types[dest] = TYPE_SET
            storage._increment_version(dest)

        return len(result)

    @staticmethod
    def sdiff(storage, *keys):
        """
        Get difference of multiple sets (first set minus all others).

        Args:
            storage: Storage instance
            *keys: bytes - set keys (first minus rest)

        Returns:
            set[bytes]: difference of sets

        Raises:
            TypeError: if any key exists but is not set type
        """
        if not keys:
            return set()

        # Validate all keys
        for key in keys:
            SetOperations._validate_type(storage, key)

        # Get first set
        result = SetOperations.smembers(storage, keys[0])

        if not result:
            return set()

        # Subtract remaining sets
        for key in keys[1:]:
            members = SetOperations.smembers(storage, key)
            result -= members  # Difference

            if not result:
                return set()  # Early exit if result becomes empty

        return result

    @staticmethod
    def sdiffstore(storage, dest, *keys):
        """
        Store difference of multiple sets in destination key.

        Args:
            storage: Storage instance
            dest: bytes - destination key
            *keys: bytes - set keys (first minus rest)

        Returns:
            int: number of members in resulting set

        Raises:
            TypeError: if any key exists but is not set type
        """
        result = SetOperations.sdiff(storage, *keys)

        # Delete destination key first
        storage.delete(dest)

        if result:
            storage.check_can_create_key(dest)
            storage._data[dest] = result
            storage._types[dest] = TYPE_SET
            storage._increment_version(dest)

        return len(result)

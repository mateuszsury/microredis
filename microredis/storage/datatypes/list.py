"""
MicroRedis List Operations

Implements Redis-compatible list operations using plain list for MicroPython
compatibility. MicroPython's ucollections.deque does NOT support len(), pop(),
list(), indexing, or iteration - so we use plain lists instead.

Memory Optimizations:
- Uses plain list (compatible with MicroPython)
- Avoids unnecessary copies with in-place operations
- Lazy materialization for range operations
"""

from microredis.storage.engine import TYPE_LIST


class ListOps:
    """
    Redis-compatible list operations using plain list.
    All methods are static to avoid instance overhead.
    """

    __slots__ = ()

    @staticmethod
    def _get_list(storage, key: bytes):
        """
        Get list from storage, validating type.

        Args:
            storage: Storage engine instance
            key: Key to retrieve

        Returns:
            list instance or None if key doesn't exist

        Raises:
            TypeError: If key exists but is not a list
        """
        if key not in storage._data:
            return None

        # Check if expired
        if storage._delete_if_expired(key):
            return None

        # Validate type
        if not storage._check_type(key, TYPE_LIST):
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

        return storage._data[key]

    @staticmethod
    def _set_list(storage, key: bytes, data: list) -> None:
        """
        Store list in storage.

        Args:
            storage: Storage engine instance
            key: Key to store
            data: list instance to store
        """
        storage.check_can_create_key(key)
        storage._data[key] = data
        storage._types[key] = TYPE_LIST
        storage._increment_version(key)

    @staticmethod
    def _normalize_index(index: int, length: int) -> int:
        """
        Normalize negative indices to positive.

        Args:
            index: Index to normalize (can be negative)
            length: Length of the list

        Returns:
            Normalized positive index or -1 if out of range
        """
        if index < 0:
            index = length + index

        if index < 0 or index >= length:
            return -1

        return index

    @staticmethod
    def lpush(storage, key: bytes, *values: bytes) -> int:
        """
        Insert values at the head of the list.

        Args:
            storage: Storage engine instance
            key: List key
            values: Values to insert (inserted left-to-right)

        Returns:
            Length of list after push
        """
        lst = ListOps._get_list(storage, key)

        if lst is None:
            lst = []

        # Push values left-to-right (rightmost value becomes new head)
        for value in values:
            lst.insert(0, value)

        ListOps._set_list(storage, key, lst)
        return len(lst)

    @staticmethod
    def rpush(storage, key: bytes, *values: bytes) -> int:
        """
        Insert values at the tail of the list.

        Args:
            storage: Storage engine instance
            key: List key
            values: Values to insert (appended left-to-right)

        Returns:
            Length of list after push
        """
        lst = ListOps._get_list(storage, key)

        if lst is None:
            lst = []

        # Append values left-to-right
        for value in values:
            lst.append(value)

        ListOps._set_list(storage, key, lst)
        return len(lst)

    @staticmethod
    def lpop(storage, key: bytes, count: int = 1):
        """
        Remove and return elements from the head of the list.

        Args:
            storage: Storage engine instance
            key: List key
            count: Number of elements to pop

        Returns:
            Single value if count=1, list if count>1, None if empty/missing
        """
        lst = ListOps._get_list(storage, key)

        if lst is None or len(lst) == 0:
            return None

        if count == 1:
            value = lst.pop(0)

            # Remove key if list is now empty
            if len(lst) == 0:
                storage.delete(key)
            else:
                ListOps._set_list(storage, key, lst)

            return value

        # Pop multiple elements
        result = []
        actual_count = min(count, len(lst))

        for _ in range(actual_count):
            result.append(lst.pop(0))

        # Remove key if list is now empty
        if len(lst) == 0:
            storage.delete(key)
        else:
            ListOps._set_list(storage, key, lst)

        return result

    @staticmethod
    def rpop(storage, key: bytes, count: int = 1):
        """
        Remove and return elements from the tail of the list.

        Args:
            storage: Storage engine instance
            key: List key
            count: Number of elements to pop

        Returns:
            Single value if count=1, list if count>1, None if empty/missing
        """
        lst = ListOps._get_list(storage, key)

        if lst is None or len(lst) == 0:
            return None

        if count == 1:
            value = lst.pop()

            # Remove key if list is now empty
            if len(lst) == 0:
                storage.delete(key)
            else:
                ListOps._set_list(storage, key, lst)

            return value

        # Pop multiple elements
        result = []
        actual_count = min(count, len(lst))

        for _ in range(actual_count):
            result.append(lst.pop())

        # Remove key if list is now empty
        if len(lst) == 0:
            storage.delete(key)
        else:
            ListOps._set_list(storage, key, lst)

        return result

    @staticmethod
    def llen(storage, key: bytes) -> int:
        """
        Get the length of a list.

        Args:
            storage: Storage engine instance
            key: List key

        Returns:
            Length of list or 0 if doesn't exist
        """
        lst = ListOps._get_list(storage, key)
        return 0 if lst is None else len(lst)

    @staticmethod
    def lrange(storage, key: bytes, start: int, stop: int) -> list:
        """
        Get a range of elements from a list.

        Args:
            storage: Storage engine instance
            key: List key
            start: Start index (can be negative)
            stop: Stop index (can be negative, inclusive)

        Returns:
            List of elements in range (empty list if key doesn't exist)
        """
        lst = ListOps._get_list(storage, key)

        if lst is None:
            return []

        length = len(lst)

        # Normalize negative indices
        if start < 0:
            start = max(0, length + start)

        if stop < 0:
            stop = length + stop
        else:
            stop = min(stop, length - 1)

        # Handle out of range
        if start > stop or start >= length:
            return []

        return lst[start:stop + 1]

    @staticmethod
    def lindex(storage, key: bytes, index: int):
        """
        Get an element from a list by index.

        Args:
            storage: Storage engine instance
            key: List key
            index: Index (can be negative)

        Returns:
            Element at index or None if out of range/key doesn't exist
        """
        lst = ListOps._get_list(storage, key)

        if lst is None:
            return None

        normalized = ListOps._normalize_index(index, len(lst))

        if normalized == -1:
            return None

        return lst[normalized]

    @staticmethod
    def lset(storage, key: bytes, index: int, value: bytes) -> bool:
        """
        Set the value of an element in a list by index.

        Args:
            storage: Storage engine instance
            key: List key
            index: Index (can be negative)
            value: New value

        Returns:
            True if successful, False if key doesn't exist or index out of range
        """
        lst = ListOps._get_list(storage, key)

        if lst is None:
            return False

        normalized = ListOps._normalize_index(index, len(lst))

        if normalized == -1:
            return False

        lst[normalized] = value
        ListOps._set_list(storage, key, lst)

        return True

    @staticmethod
    def linsert(storage, key: bytes, where: bytes, pivot: bytes, value: bytes) -> int:
        """
        Insert an element before or after a pivot element.

        Args:
            storage: Storage engine instance
            key: List key
            where: b'BEFORE' or b'AFTER'
            pivot: Pivot element to search for
            value: Value to insert

        Returns:
            Length after insertion, -1 if pivot not found, 0 if key doesn't exist
        """
        lst = ListOps._get_list(storage, key)

        if lst is None:
            return 0

        # Find pivot
        try:
            pivot_index = lst.index(pivot)
        except ValueError:
            return -1  # Pivot not found

        # Insert before or after
        if where.upper() == b'BEFORE':
            lst.insert(pivot_index, value)
        elif where.upper() == b'AFTER':
            lst.insert(pivot_index + 1, value)
        else:
            return -1  # Invalid where parameter

        ListOps._set_list(storage, key, lst)

        return len(lst)

    @staticmethod
    def lrem(storage, key: bytes, count: int, value: bytes) -> int:
        """
        Remove elements equal to value from the list.

        Args:
            storage: Storage engine instance
            key: List key
            count: Number to remove (>0: left-to-right, <0: right-to-left, 0: all)
            value: Value to remove

        Returns:
            Number of elements removed
        """
        lst = ListOps._get_list(storage, key)

        if lst is None:
            return 0

        original_len = len(lst)
        removed = 0

        if count == 0:
            # Remove all occurrences
            new_list = [x for x in lst if x != value]
            removed = original_len - len(new_list)
            lst[:] = new_list

        elif count > 0:
            # Remove first 'count' occurrences (left to right)
            new_list = []
            for item in lst:
                if item == value and removed < count:
                    removed += 1
                else:
                    new_list.append(item)
            lst[:] = new_list

        else:  # count < 0
            # Remove first 'abs(count)' occurrences (right to left)
            new_list = []
            to_remove = abs(count)
            for item in reversed(lst):
                if item == value and removed < to_remove:
                    removed += 1
                else:
                    new_list.append(item)
            lst[:] = list(reversed(new_list))

        # Update or delete key
        if len(lst) == 0:
            storage.delete(key)
        else:
            ListOps._set_list(storage, key, lst)

        return removed

    @staticmethod
    def ltrim(storage, key: bytes, start: int, stop: int) -> bool:
        """
        Trim a list to the specified range.

        Args:
            storage: Storage engine instance
            key: List key
            start: Start index (inclusive)
            stop: Stop index (inclusive)

        Returns:
            True if successful
        """
        lst = ListOps._get_list(storage, key)

        if lst is None:
            return True  # Redis returns OK even if key doesn't exist

        length = len(lst)

        # Normalize negative indices
        if start < 0:
            start = max(0, length + start)

        if stop < 0:
            stop = length + stop
        else:
            stop = min(stop, length - 1)

        # If range is invalid, delete the key
        if start > stop or start >= length:
            storage.delete(key)
            return True

        # Get trimmed range and update in-place
        trimmed = lst[start:stop + 1]
        lst[:] = trimmed
        ListOps._set_list(storage, key, lst)

        return True

    @staticmethod
    def lpos(storage, key: bytes, element: bytes):
        """
        Get the index of an element in the list.

        Args:
            storage: Storage engine instance
            key: List key
            element: Element to search for

        Returns:
            Index of first occurrence or None if not found
        """
        lst = ListOps._get_list(storage, key)

        if lst is None:
            return None

        try:
            return lst.index(element)
        except ValueError:
            return None

    # Blocking operations - not implemented yet

    @staticmethod
    def blpop(storage, keys: list, timeout: int):
        """
        Blocking LPOP - not yet implemented.

        Raises:
            NotImplementedError: Blocking operations not yet supported
        """
        raise NotImplementedError("Blocking list operations not yet implemented")

    @staticmethod
    def brpop(storage, keys: list, timeout: int):
        """
        Blocking RPOP - not yet implemented.

        Raises:
            NotImplementedError: Blocking operations not yet supported
        """
        raise NotImplementedError("Blocking list operations not yet implemented")

    @staticmethod
    def blmove(storage, source: bytes, destination: bytes, where_from: bytes,
               where_to: bytes, timeout: int):
        """
        Blocking list move - not yet implemented.

        Raises:
            NotImplementedError: Blocking operations not yet supported
        """
        raise NotImplementedError("Blocking list operations not yet implemented")

"""
Sorted Set operations for MicroRedis.

Sorted Sets store members with scores, maintaining both:
- Fast score lookup via dict: {member: score}
- Sorted order via list: [(score, member), ...]

This dual structure enables O(log n) range queries and O(1) score lookups.

Memory considerations for ESP32-S3:
- Uses bisect for efficient insertion/deletion
- Pre-allocates structures where possible
- Minimal temporary object creation
"""

import bisect

from microredis.storage.engine import TYPE_ZSET


class SortedSetOperations:
    """Sorted Set operations (all static methods)."""

    __slots__ = ()

    @staticmethod
    def _get_zset(storage, key):
        """
        Get sorted set structure.

        Returns:
            tuple[dict, list] - (scores_dict, sorted_list) or (None, None) if doesn't exist
        """
        if key not in storage._data:
            return None, None

        # Check expiry
        if storage._delete_if_expired(key):
            return None, None

        # Type check
        if not storage._check_type(key, TYPE_ZSET):
            raise TypeError("WRONGTYPE Operation against a key holding the wrong kind of value")

        return storage._data[key]

    @staticmethod
    def _set_zset(storage, key, scores, sorted_list):
        """
        Store sorted set structure.

        Args:
            storage: Storage engine
            key: Key to store at
            scores: dict[bytes, float] - member to score mapping
            sorted_list: list[(float, bytes)] - sorted by score
        """
        if not scores:
            # Empty zset, delete the key
            storage.delete(key)
            return

        storage.check_can_create_key(key)
        storage._data[key] = (scores, sorted_list)
        storage._types[key] = TYPE_ZSET
        storage._increment_version(key)

    @staticmethod
    def _remove_from_sorted(sorted_list, score, member):
        """
        Remove (score, member) from sorted list.

        Returns:
            bool - True if found and removed
        """
        # Binary search for the score
        target = (score, member)
        idx = bisect.bisect_left(sorted_list, target)

        # Check if we found it
        if idx < len(sorted_list) and sorted_list[idx] == target:
            del sorted_list[idx]
            return True

        return False

    @staticmethod
    def _add_to_sorted(sorted_list, score, member):
        """
        Add (score, member) to sorted list maintaining order.

        Uses bisect.insort for O(log n) insertion.
        """
        bisect.insort(sorted_list, (score, member))

    @staticmethod
    def _parse_score_bound(bound):
        """
        Parse score bound string.

        Args:
            bound: bytes - score bound (e.g., b"5", b"(5", b"-inf", b"+inf")

        Returns:
            tuple[float, bool] - (value, exclusive)
        """
        if isinstance(bound, (int, float)):
            return float(bound), False

        bound_str = bound.decode('utf-8') if isinstance(bound, bytes) else str(bound)
        bound_str = bound_str.strip()

        # Handle infinity
        if bound_str == '-inf':
            return float('-inf'), False
        if bound_str == '+inf' or bound_str == 'inf':
            return float('inf'), False

        # Handle exclusive bounds
        exclusive = False
        if bound_str.startswith('('):
            exclusive = True
            bound_str = bound_str[1:]

        return float(bound_str), exclusive

    @staticmethod
    def zadd(storage, key, mapping, nx=False, xx=False, gt=False, lt=False, ch=False):
        """
        Add members with scores to sorted set.

        Args:
            storage: Storage engine
            key: Key to add to
            mapping: dict[bytes, float] - member to score mapping
            nx: Only add new members (don't update existing)
            xx: Only update existing members (don't add new)
            gt: Only update if new score > current score
            lt: Only update if new score < current score
            ch: Return count of changed members (not just added)

        Returns:
            int - Number of members added (or changed if ch=True)
        """
        if not mapping:
            return 0

        scores, sorted_list = SortedSetOperations._get_zset(storage, key)

        if scores is None:
            # New sorted set
            if xx:
                # XX flag: only update existing, so return 0
                return 0

            scores = {}
            sorted_list = []

        added = 0

        for member, score in mapping.items():
            old_score = scores.get(member)
            exists = old_score is not None

            # Check flags
            if nx and exists:
                continue  # NX: don't update existing
            if xx and not exists:
                continue  # XX: don't add new
            if gt and exists and score <= old_score:
                continue  # GT: only if new score greater
            if lt and exists and score >= old_score:
                continue  # LT: only if new score less

            # Update score
            if exists:
                # Remove old entry from sorted list
                SortedSetOperations._remove_from_sorted(sorted_list, old_score, member)
                if ch and old_score != score:
                    added += 1  # CH flag: count changed members
            else:
                added += 1  # New member (always counted)

            scores[member] = score
            SortedSetOperations._add_to_sorted(sorted_list, score, member)

        SortedSetOperations._set_zset(storage, key, scores, sorted_list)
        return added

    @staticmethod
    def zrem(storage, key, *members):
        """
        Remove members from sorted set.

        Returns:
            int - Number of members removed
        """
        if not members:
            return 0

        scores, sorted_list = SortedSetOperations._get_zset(storage, key)
        if scores is None:
            return 0

        removed = 0
        for member in members:
            score = scores.get(member)
            if score is not None:
                del scores[member]
                SortedSetOperations._remove_from_sorted(sorted_list, score, member)
                removed += 1

        SortedSetOperations._set_zset(storage, key, scores, sorted_list)
        return removed

    @staticmethod
    def zscore(storage, key, member):
        """
        Get score of member.

        Returns:
            float | None - Score or None if member doesn't exist
        """
        scores, _ = SortedSetOperations._get_zset(storage, key)
        if scores is None:
            return None
        return scores.get(member)

    @staticmethod
    def zrank(storage, key, member):
        """
        Get rank (0-based index) of member in sorted set.

        Returns:
            int | None - Rank (0 = lowest score) or None if member doesn't exist
        """
        scores, sorted_list = SortedSetOperations._get_zset(storage, key)
        if scores is None:
            return None

        score = scores.get(member)
        if score is None:
            return None

        # Find member in sorted list
        target = (score, member)
        idx = bisect.bisect_left(sorted_list, target)

        if idx < len(sorted_list) and sorted_list[idx] == target:
            return idx

        return None

    @staticmethod
    def zrevrank(storage, key, member):
        """
        Get reverse rank of member (highest score = rank 0).

        Returns:
            int | None - Reverse rank or None if member doesn't exist
        """
        rank = SortedSetOperations.zrank(storage, key, member)
        if rank is None:
            return None

        _, sorted_list = SortedSetOperations._get_zset(storage, key)
        return len(sorted_list) - rank - 1

    @staticmethod
    def zcard(storage, key):
        """
        Get cardinality (number of members) in sorted set.

        Returns:
            int - Number of members
        """
        scores, _ = SortedSetOperations._get_zset(storage, key)
        if scores is None:
            return 0
        return len(scores)

    @staticmethod
    def zcount(storage, key, min_score, max_score):
        """
        Count members with scores in range [min_score, max_score].

        Args:
            min_score: Minimum score (inclusive)
            max_score: Maximum score (inclusive)

        Returns:
            int - Count of members in range
        """
        _, sorted_list = SortedSetOperations._get_zset(storage, key)
        if sorted_list is None:
            return 0

        # Parse bounds
        min_val, min_exclusive = SortedSetOperations._parse_score_bound(min_score)
        max_val, max_exclusive = SortedSetOperations._parse_score_bound(max_score)

        count = 0
        for score, _ in sorted_list:
            # Check min bound
            if min_exclusive and score <= min_val:
                continue
            if not min_exclusive and score < min_val:
                continue

            # Check max bound
            if max_exclusive and score >= max_val:
                break
            if not max_exclusive and score > max_val:
                break

            count += 1

        return count

    @staticmethod
    def zincrby(storage, key, increment, member):
        """
        Increment score of member.

        Args:
            storage: Storage engine
            key: Key
            increment: Amount to increment by
            member: Member to increment

        Returns:
            float - New score
        """
        scores, sorted_list = SortedSetOperations._get_zset(storage, key)

        if scores is None:
            # New sorted set
            scores = {}
            sorted_list = []

        old_score = scores.get(member, 0.0)
        new_score = old_score + increment

        # Update
        if member in scores:
            SortedSetOperations._remove_from_sorted(sorted_list, old_score, member)

        scores[member] = new_score
        SortedSetOperations._add_to_sorted(sorted_list, new_score, member)

        SortedSetOperations._set_zset(storage, key, scores, sorted_list)
        return new_score

    @staticmethod
    def zrange(storage, key, start, stop, withscores=False):
        """
        Get members in range [start, stop] by rank (low to high).

        Args:
            start: Start index (0-based, can be negative)
            stop: Stop index (inclusive, can be negative)
            withscores: If True, return [(member, score), ...] else [member, ...]

        Returns:
            list - Members (and optionally scores)
        """
        _, sorted_list = SortedSetOperations._get_zset(storage, key)
        if sorted_list is None:
            return []

        length = len(sorted_list)

        # Handle negative indices
        if start < 0:
            start = max(0, length + start)
        if stop < 0:
            stop = max(-1, length + stop)

        # Clamp to valid range
        start = max(0, min(start, length - 1)) if length > 0 else 0
        stop = max(-1, min(stop, length - 1))

        if start > stop or length == 0:
            return []

        # Extract range
        result = []
        for i in range(start, stop + 1):
            score, member = sorted_list[i]
            if withscores:
                result.append((member, score))
            else:
                result.append(member)

        return result

    @staticmethod
    def zrevrange(storage, key, start, stop, withscores=False):
        """
        Get members in range [start, stop] by reverse rank (high to low).

        Returns:
            list - Members in reverse order
        """
        _, sorted_list = SortedSetOperations._get_zset(storage, key)
        if sorted_list is None:
            return []

        length = len(sorted_list)

        # Handle negative indices
        if start < 0:
            start = max(0, length + start)
        if stop < 0:
            stop = max(-1, length + stop)

        # Clamp to valid range
        start = max(0, min(start, length - 1)) if length > 0 else 0
        stop = max(-1, min(stop, length - 1))

        if start > stop or length == 0:
            return []

        # Extract range in reverse order
        result = []
        for i in range(length - 1 - start, length - 1 - stop - 1, -1):
            score, member = sorted_list[i]
            if withscores:
                result.append((member, score))
            else:
                result.append(member)

        return result

    @staticmethod
    def zrangebyscore(storage, key, min_score, max_score, withscores=False, offset=0, count=None):
        """
        Get members with scores in range [min_score, max_score].

        Args:
            min_score: Minimum score (can be "-inf", "(5" for exclusive)
            max_score: Maximum score (can be "+inf", "(10" for exclusive)
            withscores: If True, return [(member, score), ...]
            offset: Skip first N results
            count: Return at most N results

        Returns:
            list - Members in score order
        """
        _, sorted_list = SortedSetOperations._get_zset(storage, key)
        if sorted_list is None:
            return []

        # Parse bounds
        min_val, min_exclusive = SortedSetOperations._parse_score_bound(min_score)
        max_val, max_exclusive = SortedSetOperations._parse_score_bound(max_score)

        result = []
        skipped = 0

        for score, member in sorted_list:
            # Check min bound
            if min_exclusive and score <= min_val:
                continue
            if not min_exclusive and score < min_val:
                continue

            # Check max bound
            if max_exclusive and score >= max_val:
                break
            if not max_exclusive and score > max_val:
                break

            # Apply offset
            if skipped < offset:
                skipped += 1
                continue

            # Add to result
            if withscores:
                result.append((member, score))
            else:
                result.append(member)

            # Check count limit
            if count is not None and len(result) >= count:
                break

        return result

    @staticmethod
    def zrevrangebyscore(storage, key, max_score, min_score, withscores=False, offset=0, count=None):
        """
        Get members with scores in range [min_score, max_score] in reverse order.

        Args:
            max_score: Maximum score (first argument in Redis)
            min_score: Minimum score (second argument in Redis)
            withscores: If True, return [(member, score), ...]
            offset: Skip first N results
            count: Return at most N results

        Returns:
            list - Members in reverse score order
        """
        _, sorted_list = SortedSetOperations._get_zset(storage, key)
        if sorted_list is None:
            return []

        # Parse bounds
        min_val, min_exclusive = SortedSetOperations._parse_score_bound(min_score)
        max_val, max_exclusive = SortedSetOperations._parse_score_bound(max_score)

        result = []
        skipped = 0

        # Iterate in reverse
        for i in range(len(sorted_list) - 1, -1, -1):
            score, member = sorted_list[i]

            # Check max bound (reversed iteration)
            if max_exclusive and score >= max_val:
                continue
            if not max_exclusive and score > max_val:
                continue

            # Check min bound (reversed iteration)
            if min_exclusive and score <= min_val:
                break
            if not min_exclusive and score < min_val:
                break

            # Apply offset
            if skipped < offset:
                skipped += 1
                continue

            # Add to result
            if withscores:
                result.append((member, score))
            else:
                result.append(member)

            # Check count limit
            if count is not None and len(result) >= count:
                break

        return result

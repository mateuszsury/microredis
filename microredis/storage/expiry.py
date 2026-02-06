"""
MicroRedis Expiry Manager Module

Min-heap based TTL management for efficient active expiry checking.
Implements Redis-like probabilistic expiry algorithm with lazy heap cleanup.

Platform: ESP32-S3 with MicroPython
RAM Constraints: ~300KB available for application

Memory Optimizations:
- __slots__ for ExpiryManager class
- Lazy heap cleanup (stale entries removed during pop)
- Min-heap for O(1) nearest expiry lookup
- Batch processing with yielding to avoid blocking
- Minimal temporary allocations

Algorithm:
1. Sample random keys from heap (EXPIRY_SAMPLE_SIZE)
2. Delete expired keys
3. If >25% were expired, repeat
4. Yield every 100 keys to allow other tasks to run
"""

import heapq
import time
import random

# Import asyncio with fallback for MicroPython
try:
    import uasyncio as asyncio
except ImportError:
    import asyncio

from microredis.core.constants import EXPIRY_CHECK_INTERVAL_MS, EXPIRY_SAMPLE_SIZE


class ExpiryManager:
    """
    Active expiry manager using min-heap for efficient TTL tracking.

    The heap contains (expire_time_ms, key) tuples and may have stale entries
    (keys that were deleted or had TTL updated). Stale entries are lazily
    removed during pop operations.

    Canonical TTL data is stored in Storage._expires dict. The heap is only
    used for efficient lookup of keys nearest to expiration.
    """

    __slots__ = ('_heap', '_storage', '_last_check')

    def __init__(self, storage):
        """
        Initialize expiry manager.

        Args:
            storage: Storage instance to manage expiries for
        """
        self._heap = []          # Min-heap of (expire_time_ms, key) tuples
        self._storage = storage  # Reference to storage engine
        self._last_check = time.ticks_ms()

    def clear(self):
        """Clear the expiry heap. Used by FLUSHDB/FLUSHALL."""
        self._heap.clear()

    def add_expiry(self, key, expire_at_ms):
        """
        Add key expiry to heap.

        Note: Does not validate if key exists. Storage is responsible for
        maintaining consistency between _expires dict and this heap.

        Args:
            key: bytes - key to track
            expire_at_ms: int - absolute expiry timestamp in milliseconds
        """
        heapq.heappush(self._heap, (expire_at_ms, key))

    def remove_expiry(self, key):
        """
        Mark key expiry as stale (lazy removal).

        Does NOT actually remove from heap to avoid O(n) search.
        Stale entries are filtered out during expire_keys() operations.

        Args:
            key: bytes - key to remove from tracking
        """
        # Lazy removal - entry will be skipped during pop
        # Storage is responsible for removing from _expires dict
        pass

    def update_expiry(self, key, expire_at_ms):
        """
        Update key expiry timestamp.

        Old entry becomes stale (lazy removal), new entry is added.

        Args:
            key: bytes - key to update
            expire_at_ms: int - new absolute expiry timestamp in milliseconds
        """
        # Add new entry (old one becomes stale and will be filtered out)
        self.add_expiry(key, expire_at_ms)

    def get_nearest_expiry(self):
        """
        Get milliseconds until nearest key expiration.

        Lazily cleans stale entries from heap top while searching.

        Returns:
            int | None: milliseconds until expiry, or None if heap is empty
        """
        now = time.ticks_ms()

        # Clean stale entries from heap top
        while self._heap:
            expire_time, key = self._heap[0]

            # Check if entry is stale (key deleted or TTL changed)
            if key not in self._storage._expires:
                # Stale entry - key was deleted
                heapq.heappop(self._heap)
                continue

            if self._storage._expires[key] != expire_time:
                # Stale entry - TTL was updated
                heapq.heappop(self._heap)
                continue

            # Valid entry - calculate time until expiry
            remaining = time.ticks_diff(expire_time, now)
            return max(0, remaining)

        # Heap is empty
        return None

    def expire_keys(self, max_count=20):
        """
        Expire keys using probabilistic Redis-like algorithm.

        Algorithm:
        1. Sample EXPIRY_SAMPLE_SIZE random keys from heap
        2. Delete expired keys
        3. If >25% were expired, repeat (indicates many expired keys)
        4. Yield every 100 keys to allow other async tasks to run

        Args:
            max_count: int - maximum keys to check per sample (default 20)

        Returns:
            int: number of keys deleted
        """
        total_deleted = 0
        iterations = 0
        now = time.ticks_ms()

        while True:
            iterations += 1
            sample_deleted = 0
            sample_checked = 0

            # Sample random keys from heap
            # We sample by popping from heap (which gives us oldest first)
            # This is more efficient than random sampling from the whole heap
            for _ in range(min(max_count, len(self._heap))):
                if not self._heap:
                    break

                expire_time, key = heapq.heappop(self._heap)
                sample_checked += 1

                # Check if entry is stale
                if key not in self._storage._expires:
                    # Stale - key was deleted, skip
                    continue

                if self._storage._expires[key] != expire_time:
                    # Stale - TTL was updated, skip
                    continue

                # Valid entry - check if expired
                remaining = time.ticks_diff(expire_time, now)

                if remaining <= 0:
                    # Expired - delete from storage
                    self._storage.delete(key)
                    sample_deleted += 1
                else:
                    # Not expired yet - put back in heap
                    heapq.heappush(self._heap, (expire_time, key))
                    # Stop processing - all remaining keys have later expiry
                    break

            total_deleted += sample_deleted

            # Check if we should continue (>25% expired)
            if sample_checked == 0:
                break

            expired_ratio = sample_deleted / sample_checked
            if expired_ratio <= 0.25:
                break

            # Yield every 100 keys to avoid blocking
            if total_deleted >= 100:
                break

        return total_deleted

    async def run_expiry_loop(self):
        """
        Main async loop for active expiry checking.

        Runs indefinitely, checking for expired keys every
        EXPIRY_CHECK_INTERVAL_MS milliseconds.

        This should be started as an async task when the server starts:
            asyncio.create_task(expiry_manager.run_expiry_loop())
        """
        while True:
            # Wait for next check interval
            await asyncio.sleep_ms(EXPIRY_CHECK_INTERVAL_MS)

            # Update last check timestamp
            self._last_check = time.ticks_ms()

            # Run expiry check
            deleted = self.expire_keys()

            # Optional: log if many keys were deleted
            # print(f"[ExpiryManager] Deleted {deleted} expired keys")

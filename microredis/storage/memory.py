"""
Memory tracking and eviction policies for MicroRedis.

Provides memory management with configurable eviction policies
optimized for ESP32-S3 RAM constraints (~300KB available).
"""

import gc
import random
try:
    from micropython import const
except ImportError:
    const = lambda x: x


# Default sample size for approximated LRU (Redis uses 5)
_SAMPLE_SIZE = const(5)


class MemoryManager:
    """
    Manages memory limits and eviction policies.

    Uses approximated LRU algorithm (sampling) to minimize RAM overhead
    while providing Redis-like eviction behavior.
    """
    __slots__ = (
        '_storage',
        '_max_memory',
        '_eviction_policy',
        '_stats',
    )

    def __init__(self, storage, max_memory=0, policy='noeviction'):
        """
        Initialize memory manager.

        Args:
            storage: Storage engine instance
            max_memory: Maximum memory in bytes (0 = no limit)
            policy: Eviction policy ('noeviction', 'allkeys-lru', 'volatile-lru',
                                     'allkeys-random', 'volatile-random')
        """
        self._storage = storage
        self._max_memory = max_memory
        self._eviction_policy = policy
        self._stats = {'evicted': 0, 'eviction_failures': 0}

        # Validate policy
        valid_policies = (
            'noeviction',
            'allkeys-lru',
            'volatile-lru',
            'allkeys-random',
            'volatile-random'
        )
        if policy not in valid_policies:
            raise ValueError(f"Invalid eviction policy: {policy}")

    def get_memory_info(self):
        """
        Get current memory statistics.

        Returns:
            dict: Memory information including usage, limits, and stats
        """
        gc.collect()  # Get accurate numbers
        used = gc.mem_alloc()
        free = gc.mem_free()

        return {
            'used_memory': used,
            'free_memory': free,
            'total_memory': used + free,
            'max_memory': self._max_memory,
            'keys': len(self._storage._data),
            'expires': len(self._storage._expires),
            'eviction_policy': self._eviction_policy,
            'evicted_keys': self._stats['evicted'],
            'eviction_failures': self._stats['eviction_failures'],
        }

    def check_memory(self):
        """
        Check if memory usage is within limits.

        Returns:
            bool: True if within limits or no limit set, False if exceeded
        """
        if self._max_memory == 0:
            return True

        # Don't call gc.collect() here - it causes 5-50ms jitter.
        # Background memory monitor already calls GC periodically.
        used = gc.mem_alloc()
        return used <= self._max_memory

    def try_evict(self, needed_bytes=0):
        """
        Attempt to evict keys to free memory.

        Args:
            needed_bytes: Minimum bytes to free (0 = just try to get under limit)

        Returns:
            bool: True if successfully evicted or no eviction needed,
                  False if eviction failed or policy is 'noeviction'
        """
        # If no memory limit, no eviction needed
        if self._max_memory == 0:
            return True

        # If we're under limit and no specific need, we're good
        if needed_bytes == 0 and self.check_memory():
            return True

        # Handle noeviction policy
        if self._eviction_policy == 'noeviction':
            if not self.check_memory():
                self._stats['eviction_failures'] = self._stats.get('eviction_failures', 0) + 1
                return False
            return True

        # Determine target: either get under max_memory or free needed_bytes
        gc.collect()
        current = gc.mem_alloc()

        if needed_bytes > 0:
            target = current - needed_bytes
        else:
            target = self._max_memory

        # Try to evict until we reach target or run out of keys
        max_attempts = 10  # Prevent infinite loops
        attempts = 0

        while gc.mem_alloc() > target and attempts < max_attempts:
            evicted = self._evict_one_key()
            if not evicted:
                # No more keys to evict
                self._stats['eviction_failures'] = self._stats.get('eviction_failures', 0) + 1
                return False

            gc.collect()
            attempts += 1

        # Check if we succeeded
        return gc.mem_alloc() <= target

    def update_stats(self):
        """Update internal statistics (for future use)."""
        # Currently stats are updated inline
        # This method is a placeholder for more complex stats tracking
        pass

    def touch_key(self, key):
        """
        Update LRU tracking for a key access.

        Args:
            key: Key that was accessed (bytes)
        """
        # For approximated LRU, we rely on storage._version
        # which is already updated on every access
        # This method is here for API compatibility
        pass

    def _evict_one_key(self):
        """
        Evict a single key based on the eviction policy.

        Returns:
            bool: True if a key was evicted, False otherwise
        """
        policy = self._eviction_policy

        if policy == 'allkeys-lru':
            return self._evict_lru(volatile_only=False)
        elif policy == 'volatile-lru':
            return self._evict_lru(volatile_only=True)
        elif policy == 'allkeys-random':
            return self._evict_random(volatile_only=False)
        elif policy == 'volatile-random':
            return self._evict_random(volatile_only=True)

        # noeviction or unknown
        return False

    def _evict_lru(self, volatile_only=False):
        """
        Evict least recently used key (approximated via sampling).

        Args:
            volatile_only: If True, only evict keys with TTL

        Returns:
            bool: True if a key was evicted, False if no candidates
        """
        # Reservoir sampling: select SAMPLE_SIZE random keys without copying all keys
        sample = []
        count = 0
        sample_size = _SAMPLE_SIZE

        for k in self._storage._data:
            if volatile_only and k not in self._storage._expires:
                continue
            count += 1
            if len(sample) < sample_size:
                sample.append(k)
            else:
                # Replace with decreasing probability
                j = random.randint(0, count - 1)
                if j < sample_size:
                    sample[j] = k

        if not sample:
            return False

        # Find key with oldest access time (least recently used)
        oldest_key = min(sample, key=lambda k: self._storage._last_access.get(k, 0))

        # Evict the key
        self._storage.delete(oldest_key)
        self._stats['evicted'] = self._stats.get('evicted', 0) + 1

        return True

    def _evict_random(self, volatile_only=False):
        """
        Evict a random key.

        Args:
            volatile_only: If True, only evict keys with TTL

        Returns:
            bool: True if a key was evicted, False if no candidates
        """
        # Reservoir sampling: pick 1 random key without copying all keys
        chosen = None
        count = 0
        for k in self._storage._data:
            if volatile_only and k not in self._storage._expires:
                continue
            count += 1
            if random.randint(1, count) == 1:
                chosen = k

        if chosen is None:
            return False

        # Evict the randomly chosen key
        self._storage.delete(chosen)
        self._stats['evicted'] = self._stats.get('evicted', 0) + 1

        return True


def estimate_key_size(key, value):
    """
    Estimate memory size of a key-value pair.

    This is an approximation including Python object overhead.

    Args:
        key: Key (bytes)
        value: Value (any type)

    Returns:
        int: Estimated size in bytes
    """
    size = 0

    # Key size (bytes object + data)
    if isinstance(key, bytes):
        size += 32 + len(key)  # Object overhead + data

    # Value size (depends on type)
    if value is None:
        size += 16
    elif isinstance(value, bytes):
        size += 32 + len(value)
    elif isinstance(value, str):
        size += 32 + len(value)
    elif isinstance(value, int):
        size += 28
    elif isinstance(value, dict):
        size += 240 + sum(estimate_key_size(k, v) for k, v in value.items())
    elif isinstance(value, list):
        size += 64 + sum(estimate_key_size(b'', item) for item in value)
    elif isinstance(value, set):
        size += 240 + sum(estimate_key_size(item, None) for item in value)
    else:
        size += 64  # Unknown type, rough estimate

    return size


def format_memory_size(bytes_size):
    """
    Format byte size as human-readable string.

    Args:
        bytes_size: Size in bytes

    Returns:
        str: Formatted size (e.g., "1.5 MB", "512 KB", "128 B")
    """
    if bytes_size < 1024:
        return f"{bytes_size} B"
    elif bytes_size < 1024 * 1024:
        kb = bytes_size / 1024.0
        return f"{kb:.1f} KB"
    elif bytes_size < 1024 * 1024 * 1024:
        mb = bytes_size / (1024.0 * 1024.0)
        return f"{mb:.1f} MB"
    else:
        gb = bytes_size / (1024.0 * 1024.0 * 1024.0)
        return f"{gb:.2f} GB"

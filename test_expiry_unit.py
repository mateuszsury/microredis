"""
Unit tests for ExpiryManager module.

Tests the min-heap based TTL management system for MicroRedis.
Platform: MicroPython/ESP32-S3
"""

import time
try:
    import uasyncio as asyncio
except ImportError:
    import asyncio

from microredis.storage.engine import Storage
from microredis.storage.expiry import ExpiryManager


class TestExpiryManager:
    """Unit tests for ExpiryManager class."""

    def setup(self):
        """Create fresh storage and expiry manager for each test."""
        self.storage = Storage()
        self.expiry_mgr = ExpiryManager(self.storage)

    def test_init(self):
        """Test ExpiryManager initialization."""
        self.setup()
        assert self.expiry_mgr._heap == []
        assert self.expiry_mgr._storage is self.storage
        assert isinstance(self.expiry_mgr._last_check, int)

    def test_add_expiry(self):
        """Test adding expiry to heap."""
        self.setup()

        # Add single expiry
        expire_time = time.ticks_add(time.ticks_ms(), 1000)
        self.expiry_mgr.add_expiry(b'key1', expire_time)

        assert len(self.expiry_mgr._heap) == 1
        assert self.expiry_mgr._heap[0] == (expire_time, b'key1')

    def test_add_multiple_expiries(self):
        """Test min-heap ordering with multiple expiries."""
        self.setup()

        now = time.ticks_ms()
        # Add in random order
        self.expiry_mgr.add_expiry(b'key3', time.ticks_add(now, 3000))
        self.expiry_mgr.add_expiry(b'key1', time.ticks_add(now, 1000))
        self.expiry_mgr.add_expiry(b'key2', time.ticks_add(now, 2000))

        # Heap should have earliest at top
        assert len(self.expiry_mgr._heap) == 3
        earliest_time, earliest_key = self.expiry_mgr._heap[0]
        assert earliest_key == b'key1'

    def test_get_nearest_expiry(self):
        """Test getting time until nearest expiry."""
        self.setup()

        # Empty heap
        assert self.expiry_mgr.get_nearest_expiry() is None

        # Add expiry
        now = time.ticks_ms()
        expire_time = time.ticks_add(now, 5000)  # 5 seconds from now

        self.storage.set(b'key1', b'value1', px=5000)
        self.expiry_mgr.add_expiry(b'key1', expire_time)

        # Should return ~5000ms
        nearest = self.expiry_mgr.get_nearest_expiry()
        assert nearest is not None
        assert 4500 <= nearest <= 5000  # Allow some timing variance

    def test_get_nearest_expiry_filters_stale(self):
        """Test that get_nearest_expiry filters stale entries."""
        self.setup()

        now = time.ticks_ms()

        # Add expiry for key that doesn't exist in storage
        self.expiry_mgr.add_expiry(b'stale_key', time.ticks_add(now, 1000))

        # Add valid expiry
        self.storage.set(b'valid_key', b'value', px=5000)
        self.expiry_mgr.add_expiry(b'valid_key', self.storage._expires[b'valid_key'])

        # Should skip stale entry and return valid one
        nearest = self.expiry_mgr.get_nearest_expiry()
        assert nearest is not None
        assert 4500 <= nearest <= 5000

    def test_update_expiry(self):
        """Test updating expiry adds new entry (old becomes stale)."""
        self.setup()

        now = time.ticks_ms()

        # Add initial expiry
        self.storage.set(b'key1', b'value1', px=1000)
        self.expiry_mgr.add_expiry(b'key1', self.storage._expires[b'key1'])

        initial_heap_size = len(self.expiry_mgr._heap)
        assert initial_heap_size == 1

        # Update expiry
        new_expire_time = time.ticks_add(now, 5000)
        self.storage._expires[b'key1'] = new_expire_time
        self.expiry_mgr.update_expiry(b'key1', new_expire_time)

        # Heap should have both entries (old is stale)
        assert len(self.expiry_mgr._heap) == 2

        # get_nearest_expiry should filter stale and return new one
        nearest = self.expiry_mgr.get_nearest_expiry()
        assert 4500 <= nearest <= 5000

    def test_remove_expiry_is_lazy(self):
        """Test that remove_expiry is lazy (no-op)."""
        self.setup()

        now = time.ticks_ms()
        self.storage.set(b'key1', b'value1', px=1000)
        self.expiry_mgr.add_expiry(b'key1', self.storage._expires[b'key1'])

        assert len(self.expiry_mgr._heap) == 1

        # Remove (should be no-op)
        self.expiry_mgr.remove_expiry(b'key1')

        # Heap still has entry
        assert len(self.expiry_mgr._heap) == 1

    def test_expire_keys_empty_heap(self):
        """Test expire_keys with empty heap."""
        self.setup()

        deleted = self.expiry_mgr.expire_keys()
        assert deleted == 0

    def test_expire_keys_no_expired(self):
        """Test expire_keys when no keys are expired."""
        self.setup()

        # Add keys with future expiry
        for i in range(5):
            key = f'key{i}'.encode()
            self.storage.set(key, b'value', px=10000)  # 10 seconds
            self.expiry_mgr.add_expiry(key, self.storage._expires[key])

        deleted = self.expiry_mgr.expire_keys()
        assert deleted == 0
        assert len(self.storage._data) == 5

    def test_expire_keys_all_expired(self):
        """Test expire_keys when all sampled keys are expired."""
        self.setup()

        # Add keys with past expiry
        now = time.ticks_ms()
        for i in range(5):
            key = f'key{i}'.encode()
            self.storage.set(key, b'value')
            # Set expiry in the past
            past_time = time.ticks_add(now, -1000)
            self.storage._expires[key] = past_time
            self.expiry_mgr.add_expiry(key, past_time)

        deleted = self.expiry_mgr.expire_keys(max_count=10)
        assert deleted == 5
        assert len(self.storage._data) == 0

    def test_expire_keys_mixed(self):
        """Test expire_keys with mix of expired and valid keys."""
        self.setup()

        now = time.ticks_ms()

        # Add expired keys
        for i in range(3):
            key = f'expired{i}'.encode()
            self.storage.set(key, b'value')
            past_time = time.ticks_add(now, -1000)
            self.storage._expires[key] = past_time
            self.expiry_mgr.add_expiry(key, past_time)

        # Add valid keys
        for i in range(3):
            key = f'valid{i}'.encode()
            self.storage.set(key, b'value', px=10000)
            self.expiry_mgr.add_expiry(key, self.storage._expires[key])

        deleted = self.expiry_mgr.expire_keys(max_count=10)
        assert deleted == 3
        assert len(self.storage._data) == 3

    def test_expire_keys_filters_stale(self):
        """Test that expire_keys filters stale heap entries."""
        self.setup()

        now = time.ticks_ms()

        # Add expiry for key
        self.storage.set(b'key1', b'value1', px=1000)
        old_expire = self.storage._expires[b'key1']
        self.expiry_mgr.add_expiry(b'key1', old_expire)

        # Update TTL in storage (makes heap entry stale)
        new_expire = time.ticks_add(now, 10000)
        self.storage._expires[b'key1'] = new_expire
        self.expiry_mgr.add_expiry(b'key1', new_expire)

        # Now heap has 2 entries, one is stale
        assert len(self.expiry_mgr._heap) == 2

        # expire_keys should filter stale entry
        deleted = self.expiry_mgr.expire_keys(max_count=10)
        assert deleted == 0  # No keys actually expired
        assert len(self.storage._data) == 1  # Key still exists

    def test_probabilistic_algorithm(self):
        """Test probabilistic expiry algorithm (>25% rule)."""
        self.setup()

        now = time.ticks_ms()

        # Add 20 keys: 5 expired (25%), 15 valid
        for i in range(5):
            key = f'expired{i}'.encode()
            self.storage.set(key, b'value')
            past_time = time.ticks_add(now, -1000)
            self.storage._expires[key] = past_time
            self.expiry_mgr.add_expiry(key, past_time)

        for i in range(15):
            key = f'valid{i}'.encode()
            self.storage.set(key, b'value', px=10000)
            self.expiry_mgr.add_expiry(key, self.storage._expires[key])

        # First batch should delete 5 keys (25% expired)
        # Algorithm should NOT continue (<=25% threshold)
        deleted = self.expiry_mgr.expire_keys(max_count=20)
        assert deleted == 5
        assert len(self.storage._data) == 15


async def test_expiry_loop():
    """Test async expiry loop."""
    storage = Storage()
    expiry_mgr = ExpiryManager(storage)

    # Add keys with short TTL
    now = time.ticks_ms()
    for i in range(5):
        key = f'key{i}'.encode()
        storage.set(key, b'value', px=100)  # 100ms TTL
        expiry_mgr.add_expiry(key, storage._expires[key])

    assert len(storage._data) == 5

    # Start expiry loop
    expiry_task = asyncio.create_task(expiry_mgr.run_expiry_loop())

    # Wait for expiry (loop runs every 100ms)
    await asyncio.sleep_ms(300)

    # All keys should be expired by now
    assert len(storage._data) == 0

    # Cleanup
    expiry_task.cancel()
    try:
        await expiry_task
    except asyncio.CancelledError:
        pass


def run_all_tests():
    """Run all synchronous tests."""
    test = TestExpiryManager()

    tests = [
        ('test_init', test.test_init),
        ('test_add_expiry', test.test_add_expiry),
        ('test_add_multiple_expiries', test.test_add_multiple_expiries),
        ('test_get_nearest_expiry', test.test_get_nearest_expiry),
        ('test_get_nearest_expiry_filters_stale', test.test_get_nearest_expiry_filters_stale),
        ('test_update_expiry', test.test_update_expiry),
        ('test_remove_expiry_is_lazy', test.test_remove_expiry_is_lazy),
        ('test_expire_keys_empty_heap', test.test_expire_keys_empty_heap),
        ('test_expire_keys_no_expired', test.test_expire_keys_no_expired),
        ('test_expire_keys_all_expired', test.test_expire_keys_all_expired),
        ('test_expire_keys_mixed', test.test_expire_keys_mixed),
        ('test_expire_keys_filters_stale', test.test_expire_keys_filters_stale),
        ('test_probabilistic_algorithm', test.test_probabilistic_algorithm),
    ]

    passed = 0
    failed = 0

    print("Running ExpiryManager unit tests...\n")

    for name, test_func in tests:
        try:
            test_func()
            print(f"✓ {name}")
            passed += 1
        except AssertionError as e:
            print(f"✗ {name}: {e}")
            failed += 1
        except Exception as e:
            print(f"✗ {name}: ERROR - {e}")
            failed += 1

    print(f"\n{passed} passed, {failed} failed")

    return failed == 0


async def run_async_tests():
    """Run all async tests."""
    print("\nRunning async tests...\n")

    try:
        await test_expiry_loop()
        print("✓ test_expiry_loop")
        return True
    except AssertionError as e:
        print(f"✗ test_expiry_loop: {e}")
        return False
    except Exception as e:
        print(f"✗ test_expiry_loop: ERROR - {e}")
        return False


if __name__ == '__main__':
    # Run synchronous tests
    sync_passed = run_all_tests()

    # Run async tests
    try:
        async_passed = asyncio.run(run_async_tests())
    except AttributeError:
        # MicroPython doesn't have asyncio.run()
        loop = asyncio.get_event_loop()
        async_passed = loop.run_until_complete(run_async_tests())

    if sync_passed and async_passed:
        print("\n✓ All tests passed!")
    else:
        print("\n✗ Some tests failed")

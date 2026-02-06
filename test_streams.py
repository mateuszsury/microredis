"""
Test script for Redis Streams implementation in MicroRedis.

Platform: ESP32-S3 with MicroPython (also compatible with CPython for testing)
"""

import sys
import time

# Add parent directory to path for imports
sys.path.insert(0, 'C:\\Users\\thete\\OneDrive\\Dokumenty\\PyCharm\\MicroRedis')

from microredis.storage.engine import Storage
from microredis.commands.streams import StreamOperations


def test_xadd_and_xlen():
    """Test XADD and XLEN commands."""
    print("Test 1: XADD and XLEN")
    storage = Storage()

    # Add entry with auto-generated ID
    entry_id1 = StreamOperations.xadd(storage, b'mystream', b'*', {
        b'sensor': b'temp',
        b'value': b'23.5',
        b'unit': b'C'
    })
    print(f"  Added entry 1: {entry_id1}")

    # Add another entry
    time.sleep(0.001)  # Wait 1ms to ensure different timestamp
    entry_id2 = StreamOperations.xadd(storage, b'mystream', b'*', {
        b'sensor': b'humidity',
        b'value': b'65',
        b'unit': b'%'
    })
    print(f"  Added entry 2: {entry_id2}")

    # Check length
    length = StreamOperations.xlen(storage, b'mystream')
    print(f"  Stream length: {length}")
    assert length == 2, f"Expected length 2, got {length}"

    print("  PASS\n")


def test_xrange():
    """Test XRANGE command."""
    print("Test 2: XRANGE")
    storage = Storage()

    # Add multiple entries
    ids = []
    for i in range(5):
        entry_id = StreamOperations.xadd(storage, b'mystream', b'*', {
            b'index': str(i).encode(),
            b'data': f'value{i}'.encode()
        })
        ids.append(entry_id)
        time.sleep(0.001)

    print(f"  Added 5 entries: {[id.decode() for id in ids]}")

    # Get all entries
    entries = StreamOperations.xrange(storage, b'mystream', b'-', b'+')
    print(f"  XRANGE - +: {len(entries)} entries")
    assert len(entries) == 5, f"Expected 5 entries, got {len(entries)}"

    # Get limited entries
    entries = StreamOperations.xrange(storage, b'mystream', b'-', b'+', count=3)
    print(f"  XRANGE - + COUNT 3: {len(entries)} entries")
    assert len(entries) == 3, f"Expected 3 entries, got {len(entries)}"

    # Get range
    entries = StreamOperations.xrange(storage, b'mystream', ids[1], ids[3])
    print(f"  XRANGE {ids[1].decode()} {ids[3].decode()}: {len(entries)} entries")
    assert len(entries) == 3, f"Expected 3 entries, got {len(entries)}"

    print("  PASS\n")


def test_xrevrange():
    """Test XREVRANGE command."""
    print("Test 3: XREVRANGE")
    storage = Storage()

    # Add entries
    ids = []
    for i in range(5):
        entry_id = StreamOperations.xadd(storage, b'mystream', b'*', {
            b'index': str(i).encode()
        })
        ids.append(entry_id)
        time.sleep(0.001)

    # Get entries in reverse
    entries = StreamOperations.xrevrange(storage, b'mystream', b'+', b'-')
    print(f"  XREVRANGE + -: {len(entries)} entries")
    assert len(entries) == 5, f"Expected 5 entries, got {len(entries)}"

    # Verify order is reversed
    first_entry_id = entries[0][0]
    last_entry_id = entries[-1][0]
    print(f"  First entry (newest): {first_entry_id.decode()}")
    print(f"  Last entry (oldest): {last_entry_id.decode()}")
    assert first_entry_id == ids[4], "First entry should be newest"
    assert last_entry_id == ids[0], "Last entry should be oldest"

    print("  PASS\n")


def test_xread():
    """Test XREAD command."""
    print("Test 4: XREAD")
    storage = Storage()

    # Add entries to stream1
    id1 = StreamOperations.xadd(storage, b'stream1', b'*', {b'msg': b'hello'})
    id2 = StreamOperations.xadd(storage, b'stream1', b'*', {b'msg': b'world'})

    # Add entries to stream2
    id3 = StreamOperations.xadd(storage, b'stream2', b'*', {b'msg': b'foo'})
    id4 = StreamOperations.xadd(storage, b'stream2', b'*', {b'msg': b'bar'})

    # Read from both streams after specific IDs
    result = StreamOperations.xread(storage, {
        b'stream1': b'0-0',
        b'stream2': b'0-0'
    })

    print(f"  XREAD result: {len(result)} streams")
    assert result is not None, "XREAD should return results"
    assert len(result) == 2, f"Expected 2 streams, got {len(result)}"

    # Check stream1
    stream1_data = [item for item in result if item[0] == b'stream1'][0]
    print(f"  Stream1 entries: {len(stream1_data[1])}")
    assert len(stream1_data[1]) == 2, "Stream1 should have 2 entries"

    # Check stream2
    stream2_data = [item for item in result if item[0] == b'stream2'][0]
    print(f"  Stream2 entries: {len(stream2_data[1])}")
    assert len(stream2_data[1]) == 2, "Stream2 should have 2 entries"

    # Read with COUNT limit
    result = StreamOperations.xread(storage, {
        b'stream1': b'0-0',
        b'stream2': b'0-0'
    }, count=1)

    stream1_data = [item for item in result if item[0] == b'stream1'][0]
    print(f"  Stream1 entries (COUNT 1): {len(stream1_data[1])}")
    assert len(stream1_data[1]) == 1, "Stream1 should have 1 entry with COUNT 1"

    print("  PASS\n")


def test_xtrim():
    """Test XTRIM command."""
    print("Test 5: XTRIM")
    storage = Storage()

    # Add 10 entries
    for i in range(10):
        StreamOperations.xadd(storage, b'mystream', b'*', {
            b'index': str(i).encode()
        })
        time.sleep(0.001)

    length = StreamOperations.xlen(storage, b'mystream')
    print(f"  Initial length: {length}")
    assert length == 10, f"Expected 10 entries, got {length}"

    # Trim to 5 entries
    deleted = StreamOperations.xtrim(storage, b'mystream', 5)
    print(f"  Trimmed {deleted} entries")
    assert deleted == 5, f"Expected 5 deleted, got {deleted}"

    # Check new length
    length = StreamOperations.xlen(storage, b'mystream')
    print(f"  New length: {length}")
    assert length == 5, f"Expected 5 entries, got {length}"

    # Verify oldest entries were removed (should have indices 5-9)
    entries = StreamOperations.xrange(storage, b'mystream', b'-', b'+')
    first_index = entries[0][1][b'index'].decode()
    last_index = entries[-1][1][b'index'].decode()
    print(f"  Remaining entries: {first_index} to {last_index}")
    assert first_index == '5', f"Expected first index 5, got {first_index}"
    assert last_index == '9', f"Expected last index 9, got {last_index}"

    print("  PASS\n")


def test_error_handling():
    """Test error handling."""
    print("Test 6: Error Handling")
    storage = Storage()

    # Add entry with auto ID
    id1 = StreamOperations.xadd(storage, b'mystream', b'*', {b'data': b'test'})
    print(f"  Added entry: {id1.decode()}")

    # Try to add entry with smaller ID
    try:
        StreamOperations.xadd(storage, b'mystream', b'1000-0', {b'data': b'test'})
        assert False, "Should have raised ValueError"
    except ValueError as e:
        print(f"  Correctly rejected smaller ID: {e}")

    # Try to add entry with invalid ID format
    try:
        StreamOperations.xadd(storage, b'mystream', b'invalid', {b'data': b'test'})
        assert False, "Should have raised ValueError"
    except ValueError as e:
        print(f"  Correctly rejected invalid ID: {e}")

    # Store non-stream data
    storage.set(b'notastream', b'just a string')

    # Try to use stream operations on non-stream key
    try:
        StreamOperations.xadd(storage, b'notastream', b'*', {b'data': b'test'})
        assert False, "Should have raised ValueError for wrong type"
    except ValueError as e:
        print(f"  Correctly rejected wrong type: {e}")

    print("  PASS\n")


def test_memory_efficiency():
    """Test memory efficiency with many entries."""
    print("Test 7: Memory Efficiency")
    storage = Storage()

    # Add 100 entries
    print("  Adding 100 entries...")
    for i in range(100):
        StreamOperations.xadd(storage, b'bigstream', b'*', {
            b'index': str(i).encode(),
            b'data': f'data-{i}'.encode(),
            b'timestamp': str(int(time.time() * 1000)).encode()
        })
        if i % 10 == 0:
            time.sleep(0.001)

    length = StreamOperations.xlen(storage, b'bigstream')
    print(f"  Stream length: {length}")
    assert length == 100, f"Expected 100 entries, got {length}"

    # Read in chunks
    entries = StreamOperations.xrange(storage, b'bigstream', b'-', b'+', count=10)
    print(f"  Read first 10 entries: {len(entries)} entries")
    assert len(entries) == 10, f"Expected 10 entries, got {len(entries)}"

    # Trim to manageable size
    deleted = StreamOperations.xtrim(storage, b'bigstream', 20)
    print(f"  Trimmed to 20 entries, deleted {deleted}")
    assert deleted == 80, f"Expected 80 deleted, got {deleted}"

    print("  PASS\n")


def run_all_tests():
    """Run all tests."""
    print("=" * 60)
    print("MicroRedis Streams Test Suite")
    print("=" * 60)
    print()

    tests = [
        test_xadd_and_xlen,
        test_xrange,
        test_xrevrange,
        test_xread,
        test_xtrim,
        test_error_handling,
        test_memory_efficiency,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"  FAIL: {e}\n")
            failed += 1
        except Exception as e:
            print(f"  ERROR: {e}\n")
            failed += 1

    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    return failed == 0


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)

"""
MicroRedis Test Runner

Simple test framework for MicroRedis that works on both CPython and MicroPython.
No external dependencies required.
"""

import sys
import gc

# Detect platform
try:
    import micropython
    IS_MICROPYTHON = True
except ImportError:
    IS_MICROPYTHON = False

# Track test results
_passed = 0
_failed = 0
_test_name = ""


def test_start(name):
    """Begin a test case"""
    global _test_name
    _test_name = name


def assert_equal(actual, expected, msg=None):
    """Assert that actual equals expected"""
    global _passed, _failed
    if actual == expected:
        _passed += 1
        print(f"[PASS] {_test_name}")
        return True
    else:
        _failed += 1
        error_msg = msg or f"expected {expected!r}, got {actual!r}"
        print(f"[FAIL] {_test_name}: {error_msg}")
        return False


def assert_true(condition, msg="condition is False"):
    """Assert that condition is True"""
    return assert_equal(condition, True, msg)


def assert_not_none(value, msg="value is None"):
    """Assert that value is not None"""
    global _passed, _failed
    if value is not None:
        _passed += 1
        print(f"[PASS] {_test_name}")
        return True
    else:
        _failed += 1
        print(f"[FAIL] {_test_name}: {msg}")
        return False


def assert_none(value, msg=None):
    """Assert that value is None"""
    return assert_equal(value, None, msg or f"expected None, got {value!r}")


def assert_raises(exception_type, func, *args, **kwargs):
    """Assert that func raises exception_type"""
    global _passed, _failed
    try:
        func(*args, **kwargs)
        _failed += 1
        print(f"[FAIL] {_test_name}: expected {exception_type.__name__} but no exception raised")
        return False
    except exception_type:
        _passed += 1
        print(f"[PASS] {_test_name}")
        return True
    except Exception as e:
        _failed += 1
        print(f"[FAIL] {_test_name}: expected {exception_type.__name__}, got {type(e).__name__}")
        return False


def test_module_imports():
    """Test that all core modules can be imported"""
    test_start("Module imports: core.constants")
    try:
        from microredis.core import constants
        assert_not_none(constants.BUFFER_SIZE)
    except Exception as e:
        assert_true(False, f"import failed: {e}")

    test_start("Module imports: core.protocol")
    try:
        from microredis.core.protocol import RESPParser
        assert_not_none(RESPParser)
    except Exception as e:
        assert_true(False, f"import failed: {e}")

    test_start("Module imports: core.response")
    try:
        from microredis.core.response import ResponseBuilder
        assert_not_none(ResponseBuilder)
    except Exception as e:
        assert_true(False, f"import failed: {e}")

    test_start("Module imports: storage.engine")
    try:
        from microredis.storage.engine import Storage
        assert_not_none(Storage)
    except Exception as e:
        assert_true(False, f"import failed: {e}")


def test_storage_engine():
    """Test basic storage engine operations"""
    from microredis.storage.engine import Storage

    storage = Storage()

    # Test SET and GET
    test_start("Storage: SET key value")
    result = storage.set(b'testkey', b'testvalue')
    assert_true(result)

    test_start("Storage: GET existing key")
    value = storage.get(b'testkey')
    assert_equal(value, b'testvalue')

    test_start("Storage: GET non-existent key")
    value = storage.get(b'nonexistent')
    assert_none(value)

    # Test EXISTS
    test_start("Storage: EXISTS on existing key")
    count = storage.exists(b'testkey')
    assert_equal(count, 1)

    test_start("Storage: EXISTS on multiple keys")
    storage.set(b'key2', b'value2')
    count = storage.exists(b'testkey', b'key2', b'nonexistent')
    assert_equal(count, 2)

    # Test DEL
    test_start("Storage: DEL existing key")
    count = storage.delete(b'testkey')
    assert_equal(count, 1)

    test_start("Storage: DEL non-existent key")
    count = storage.delete(b'nonexistent')
    assert_equal(count, 0)

    test_start("Storage: GET after DEL")
    value = storage.get(b'testkey')
    assert_none(value)


def test_protocol_parser():
    """Test RESP2 protocol parser"""
    from microredis.core.protocol import RESPParser

    parser = RESPParser()

    # Test simple string
    test_start("Protocol: parse simple string")
    data = b'+OK\r\n'
    parser.feed(data)
    result = parser.parse()
    assert_not_none(result)

    # Test bulk string command
    test_start("Protocol: parse PING command")
    parser = RESPParser()
    data = b'*1\r\n$4\r\nPING\r\n'
    parser.feed(data)
    result = parser.parse()
    if result:
        cmd, args = result
        assert_equal(cmd, 'PING')
    else:
        assert_true(False, "failed to parse PING")

    # Test SET command with arguments
    test_start("Protocol: parse SET command")
    parser = RESPParser()
    data = b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n'
    parser.feed(data)
    result = parser.parse()
    if result:
        cmd, args = result
        assert_equal(cmd, 'SET')
        if len(args) >= 2:
            assert_equal(args[0], b'key')
            assert_equal(args[1], b'value')
        else:
            assert_true(False, f"expected 2 args, got {len(args)}")
    else:
        assert_true(False, "failed to parse SET")

    # Test incomplete message
    test_start("Protocol: handle incomplete message")
    parser = RESPParser()
    data = b'*1\r\n$4\r\nPI'  # incomplete
    parser.feed(data)
    result = parser.parse()
    assert_none(result, "should return None for incomplete message")


def test_response_builder():
    """Test RESP2 response builder"""
    from microredis.core.response import (
        simple_string, error, integer, bulk_string,
        bulk_string_or_null, array, ResponseBuilder,
        RESP_OK, RESP_PONG, RESP_NULL
    )

    # Test pre-allocated responses
    test_start("Response: RESP_OK constant")
    assert_equal(RESP_OK, b'+OK\r\n')

    test_start("Response: RESP_PONG constant")
    assert_equal(RESP_PONG, b'+PONG\r\n')

    test_start("Response: RESP_NULL constant")
    assert_equal(RESP_NULL, b'$-1\r\n')

    # Test simple builders
    test_start("Response: simple_string()")
    result = simple_string(b'HELLO')
    assert_equal(result, b'+HELLO\r\n')

    test_start("Response: error()")
    result = error(b'ERR unknown')
    assert_equal(result, b'-ERR unknown\r\n')

    test_start("Response: integer()")
    result = integer(42)
    assert_equal(result, b':42\r\n')

    test_start("Response: bulk_string()")
    result = bulk_string(b'hello')
    assert_equal(result, b'$5\r\nhello\r\n')

    test_start("Response: bulk_string_or_null() with None")
    result = bulk_string_or_null(None)
    assert_equal(result, b'$-1\r\n')

    test_start("Response: bulk_string_or_null() with value")
    result = bulk_string_or_null(b'test')
    assert_equal(result, b'$4\r\ntest\r\n')

    # Test array
    test_start("Response: array()")
    items = [bulk_string(b'foo'), bulk_string(b'bar')]
    result = array(items)
    assert_equal(result, b'*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n')

    # Test ResponseBuilder
    test_start("Response: ResponseBuilder.add_bulk()")
    builder = ResponseBuilder()
    builder.add_bulk(b'test')
    result = builder.get_response()
    assert_equal(result, b'$4\r\ntest\r\n')


def test_datatypes_string():
    """Test string datatype operations"""
    from microredis.storage.engine import Storage
    from microredis.storage.datatypes.string import StringOperations

    storage = Storage()

    # Test APPEND
    test_start("String: APPEND to new key")
    storage.set(b'mykey', b'Hello')
    length = StringOperations.append(storage, b'mykey', b' World')
    assert_equal(length, 11)

    test_start("String: GET after APPEND")
    value = storage.get(b'mykey')
    assert_equal(value, b'Hello World')

    # Test STRLEN
    test_start("String: STRLEN")
    length = StringOperations.strlen(storage, b'mykey')
    assert_equal(length, 11)

    test_start("String: STRLEN on non-existent key")
    length = StringOperations.strlen(storage, b'nonexistent')
    assert_equal(length, 0)

    # Test INCR/DECR
    test_start("String: INCR")
    storage.set(b'counter', b'10')
    result = StringOperations.incr(storage, b'counter')
    assert_equal(result, 11)

    test_start("String: DECR")
    result = StringOperations.decr(storage, b'counter')
    assert_equal(result, 10)

    test_start("String: INCRBY")
    result = StringOperations.incrby(storage, b'counter', 5)
    assert_equal(result, 15)

    # Test GETRANGE
    test_start("String: GETRANGE")
    storage.set(b'mykey', b'This is a string')
    result = StringOperations.getrange(storage, b'mykey', 0, 3)
    assert_equal(result, b'This')

    # Test MGET/MSET
    test_start("String: MSET")
    result = StringOperations.mset(storage, {b'key1': b'val1', b'key2': b'val2'})
    assert_true(result)

    test_start("String: MGET")
    values = StringOperations.mget(storage, b'key1', b'key2', b'nonexistent')
    assert_equal(len(values), 3)
    assert_equal(values[0], b'val1')
    assert_equal(values[1], b'val2')
    assert_none(values[2])


def test_datatypes_hash():
    """Test hash datatype operations"""
    from microredis.storage.engine import Storage
    from microredis.storage.datatypes.hash import HashType as HashOperations

    storage = Storage()

    # Test HSET/HGET
    test_start("Hash: HSET single field")
    result = HashOperations.hset(storage, b'myhash', b'field1', b'value1')
    assert_equal(result, 1)

    test_start("Hash: HGET existing field")
    value = HashOperations.hget(storage, b'myhash', b'field1')
    assert_equal(value, b'value1')

    test_start("Hash: HGET non-existent field")
    value = HashOperations.hget(storage, b'myhash', b'nonexistent')
    assert_none(value)

    # Test HDEL
    test_start("Hash: HDEL")
    count = HashOperations.hdel(storage, b'myhash', b'field1')
    assert_equal(count, 1)

    # Test HEXISTS
    test_start("Hash: HEXISTS after DEL")
    exists = HashOperations.hexists(storage, b'myhash', b'field1')
    assert_equal(exists, 0)

    # Test HLEN
    test_start("Hash: HLEN")
    HashOperations.hset(storage, b'myhash', b'f1', b'v1')
    HashOperations.hset(storage, b'myhash', b'f2', b'v2')
    length = HashOperations.hlen(storage, b'myhash')
    assert_equal(length, 2)


def test_datatypes_list():
    """Test list datatype operations"""
    from microredis.storage.engine import Storage
    from microredis.storage.datatypes.list import ListOps as ListOperations

    storage = Storage()

    # Test LPUSH/RPUSH
    test_start("List: LPUSH")
    length = ListOperations.lpush(storage, b'mylist', b'world')
    assert_equal(length, 1)

    test_start("List: LPUSH multiple")
    length = ListOperations.lpush(storage, b'mylist', b'hello')
    assert_equal(length, 2)

    # Test LLEN
    test_start("List: LLEN")
    length = ListOperations.llen(storage, b'mylist')
    assert_equal(length, 2)

    # Test LPOP/RPOP
    test_start("List: LPOP")
    value = ListOperations.lpop(storage, b'mylist')
    assert_equal(value, b'hello')

    test_start("List: LLEN after LPOP")
    length = ListOperations.llen(storage, b'mylist')
    assert_equal(length, 1)

    # Test LRANGE
    test_start("List: LRANGE")
    ListOperations.rpush(storage, b'mylist', b'a')
    ListOperations.rpush(storage, b'mylist', b'b')
    values = ListOperations.lrange(storage, b'mylist', 0, -1)
    assert_equal(len(values), 3)


def test_datatypes_set():
    """Test set datatype operations"""
    from microredis.storage.engine import Storage
    from microredis.storage.datatypes.set import SetOperations

    storage = Storage()

    # Test SADD
    test_start("Set: SADD new member")
    count = SetOperations.sadd(storage, b'myset', b'member1')
    assert_equal(count, 1)

    test_start("Set: SADD duplicate member")
    count = SetOperations.sadd(storage, b'myset', b'member1')
    assert_equal(count, 0)

    # Test SISMEMBER
    test_start("Set: SISMEMBER existing")
    result = SetOperations.sismember(storage, b'myset', b'member1')
    assert_equal(result, 1)

    test_start("Set: SISMEMBER non-existent")
    result = SetOperations.sismember(storage, b'myset', b'nonexistent')
    assert_equal(result, 0)

    # Test SCARD
    test_start("Set: SCARD")
    SetOperations.sadd(storage, b'myset', b'member2')
    card = SetOperations.scard(storage, b'myset')
    assert_equal(card, 2)

    # Test SREM
    test_start("Set: SREM")
    count = SetOperations.srem(storage, b'myset', b'member1')
    assert_equal(count, 1)


def test_datatypes_zset():
    """Test sorted set datatype operations"""
    from microredis.storage.engine import Storage
    from microredis.storage.datatypes.zset import SortedSetOperations as ZSetOperations

    storage = Storage()

    # Test ZADD (mapping is dict: {member: score})
    test_start("ZSet: ZADD new member")
    count = ZSetOperations.zadd(storage, b'myzset', {b'member1': 1.0})
    assert_equal(count, 1)

    test_start("ZSet: ZADD another member")
    count = ZSetOperations.zadd(storage, b'myzset', {b'member2': 2.0})
    assert_equal(count, 1)

    # Test ZSCORE
    test_start("ZSet: ZSCORE existing")
    score = ZSetOperations.zscore(storage, b'myzset', b'member1')
    assert_equal(score, 1.0)

    test_start("ZSet: ZSCORE non-existent")
    score = ZSetOperations.zscore(storage, b'myzset', b'nonexistent')
    assert_none(score)

    # Test ZCARD
    test_start("ZSet: ZCARD")
    card = ZSetOperations.zcard(storage, b'myzset')
    assert_equal(card, 2)

    # Test ZRANK
    test_start("ZSet: ZRANK")
    rank = ZSetOperations.zrank(storage, b'myzset', b'member1')
    assert_equal(rank, 0)

    # Test ZREM
    test_start("ZSet: ZREM")
    count = ZSetOperations.zrem(storage, b'myzset', b'member1')
    assert_equal(count, 1)


def test_expiry():
    """Test key expiration"""
    from microredis.storage.engine import Storage
    import time

    storage = Storage()

    # Test SET with EX (seconds)
    test_start("Expiry: SET with EX option")
    result = storage.set(b'tempkey', b'tempvalue', ex=1)
    assert_true(result)

    test_start("Expiry: GET before expiration")
    value = storage.get(b'tempkey')
    assert_equal(value, b'tempvalue')

    # Wait for expiration (only on platforms with sleep)
    if not IS_MICROPYTHON or hasattr(time, 'sleep'):
        test_start("Expiry: GET after expiration")
        time.sleep(1.1)
        value = storage.get(b'tempkey')
        assert_none(value, "key should have expired")

    # Test SET with PX (milliseconds) - no sleep needed, just check it accepts it
    test_start("Expiry: SET with PX option")
    result = storage.set(b'tempkey2', b'value', px=100)
    assert_true(result)


def test_memory_info():
    """Test memory information retrieval"""
    test_start("Memory: gc.mem_free()")
    try:
        free = gc.mem_free()
        assert_true(free > 0, f"mem_free should be positive, got {free}")
    except AttributeError:
        # CPython doesn't have mem_free
        assert_true(True, "skipped on CPython")

    test_start("Memory: gc.mem_alloc()")
    try:
        alloc = gc.mem_alloc()
        assert_true(alloc > 0, f"mem_alloc should be positive, got {alloc}")
    except AttributeError:
        # CPython doesn't have mem_alloc
        assert_true(True, "skipped on CPython")

    test_start("Memory: gc.collect()")
    try:
        gc.collect()
        assert_true(True)
    except Exception as e:
        assert_true(False, f"gc.collect() failed: {e}")


def run_all_tests():
    """Run all test suites"""
    global _passed, _failed
    _passed = 0
    _failed = 0

    print("MicroRedis Test Runner v1.0")
    print("=" * 40)
    print(f"Platform: {'MicroPython' if IS_MICROPYTHON else 'CPython'}")
    print(f"Python: {sys.version}")
    print("=" * 40)
    print()

    # Run all test suites
    try:
        test_module_imports()
        test_storage_engine()
        test_protocol_parser()
        test_response_builder()
        test_datatypes_string()
        test_datatypes_hash()
        test_datatypes_list()
        test_datatypes_set()
        test_datatypes_zset()
        test_expiry()
        test_memory_info()
    except Exception as e:
        print(f"\n[ERROR] Test suite crashed: {e}")
        if hasattr(sys, 'print_exception'):
            sys.print_exception(e)
        else:
            import traceback
            traceback.print_exc()

    # Print summary
    print()
    print("=" * 40)
    total = _passed + _failed
    print(f"Results: {_passed} passed, {_failed} failed (total: {total})")

    if _failed == 0:
        print("All tests passed!")
        return 0
    else:
        print(f"{_failed} test(s) failed")
        return 1


if __name__ == '__main__':
    exit_code = run_all_tests()
    sys.exit(exit_code)

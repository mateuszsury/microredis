"""
Test script for microredis/core/response.py

Validates RESP2 response building functionality.
Run with: python test_response.py or micropython test_response.py
"""

from microredis.core.response import (
    # Pre-allocated responses
    RESP_OK, RESP_PONG, RESP_NULL, RESP_NULL_ARRAY, RESP_EMPTY_ARRAY,
    RESP_ZERO, RESP_ONE, RESP_QUEUED,
    # Pre-allocated errors
    ERR_UNKNOWN_CMD, ERR_WRONG_ARITY, ERR_WRONGTYPE, ERR_SYNTAX,
    ERR_NOT_INTEGER,
    # Response building functions
    simple_string, error, error_wrongtype, error_syntax,
    integer, bulk_string, bulk_string_or_null, array, encode_value,
    # ResponseBuilder class
    ResponseBuilder
)


def test_preallocated_responses():
    """Test pre-allocated constant responses."""
    print("Testing pre-allocated responses...")

    assert RESP_OK == b'+OK\r\n'
    assert RESP_PONG == b'+PONG\r\n'
    assert RESP_NULL == b'$-1\r\n'
    assert RESP_NULL_ARRAY == b'*-1\r\n'
    assert RESP_EMPTY_ARRAY == b'*0\r\n'
    assert RESP_ZERO == b':0\r\n'
    assert RESP_ONE == b':1\r\n'
    assert RESP_QUEUED == b'+QUEUED\r\n'

    print("  [OK] All pre-allocated responses correct")


def test_preallocated_errors():
    """Test pre-allocated error responses."""
    print("Testing pre-allocated errors...")

    assert ERR_UNKNOWN_CMD == b'-ERR unknown command\r\n'
    assert ERR_WRONG_ARITY == b'-ERR wrong number of arguments\r\n'
    assert ERR_WRONGTYPE == b'-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'
    assert ERR_SYNTAX == b'-ERR syntax error\r\n'
    assert ERR_NOT_INTEGER == b'-ERR value is not an integer or out of range\r\n'

    # Test error helper functions
    assert error_wrongtype() == ERR_WRONGTYPE
    assert error_syntax() == ERR_SYNTAX

    print("  [OK] All pre-allocated errors correct")


def test_simple_string():
    """Test simple string response builder."""
    print("Testing simple_string()...")

    assert simple_string('OK') == b'+OK\r\n'
    assert simple_string('PONG') == b'+PONG\r\n'
    assert simple_string('test message') == b'+test message\r\n'

    # Test with bytes input
    assert simple_string(b'binary') == b'+binary\r\n'

    print("  [OK] simple_string() working correctly")


def test_error():
    """Test error response builder."""
    print("Testing error()...")

    assert error('unknown command') == b'-unknown command\r\n'
    assert error('syntax error') == b'-syntax error\r\n'

    # Test with bytes input
    assert error(b'test error') == b'-test error\r\n'

    print("  [OK] error() working correctly")


def test_integer():
    """Test integer response builder."""
    print("Testing integer()...")

    # Test pre-allocated common integers
    assert integer(0) == RESP_ZERO
    assert integer(1) == RESP_ONE

    # Test other integers
    assert integer(42) == b':42\r\n'
    assert integer(-100) == b':-100\r\n'
    assert integer(1000) == b':1000\r\n'

    print("  [OK] integer() working correctly")


def test_bulk_string():
    """Test bulk string response builder."""
    print("Testing bulk_string()...")

    assert bulk_string(b'hello') == b'$5\r\nhello\r\n'
    assert bulk_string(b'') == b'$0\r\n\r\n'
    assert bulk_string(b'test\r\ndata') == b'$10\r\ntest\r\ndata\r\n'

    print("  [OK] bulk_string() working correctly")


def test_bulk_string_or_null():
    """Test bulk string or null response builder."""
    print("Testing bulk_string_or_null()...")

    assert bulk_string_or_null(None) == RESP_NULL
    assert bulk_string_or_null(b'test') == b'$4\r\ntest\r\n'
    assert bulk_string_or_null(b'') == b'$0\r\n\r\n'

    print("  [OK] bulk_string_or_null() working correctly")


def test_array():
    """Test array response builder."""
    print("Testing array()...")

    # Empty array
    assert array([]) == RESP_EMPTY_ARRAY

    # Single element
    assert array([b':1\r\n']) == b'*1\r\n:1\r\n'

    # Multiple elements
    items = [b':1\r\n', b'$4\r\ntest\r\n', b'+OK\r\n']
    expected = b'*3\r\n:1\r\n$4\r\ntest\r\n+OK\r\n'
    assert array(items) == expected

    print("  [OK] array() working correctly")


def test_encode_value():
    """Test automatic value encoding."""
    print("Testing encode_value()...")

    # None -> null
    assert encode_value(None) == RESP_NULL

    # Integer
    assert encode_value(0) == RESP_ZERO
    assert encode_value(42) == b':42\r\n'

    # String -> bulk string
    assert encode_value('hello') == b'$5\r\nhello\r\n'

    # Bytes -> bulk string
    assert encode_value(b'world') == b'$5\r\nworld\r\n'

    # List -> array
    result = encode_value([1, 'test', b'data'])
    expected = b'*3\r\n:1\r\n$4\r\ntest\r\n$4\r\ndata\r\n'
    assert result == expected

    # Nested list
    result = encode_value([1, [2, 3]])
    expected = b'*2\r\n:1\r\n*2\r\n:2\r\n:3\r\n'
    assert result == expected

    print("  [OK] encode_value() working correctly")


def test_response_builder():
    """Test ResponseBuilder class."""
    print("Testing ResponseBuilder...")

    builder = ResponseBuilder()

    # Test simple string
    builder.add_simple('OK')
    assert builder.get_response() == b'+OK\r\n'

    # Test error
    builder.add_error('test error')
    assert builder.get_response() == b'-test error\r\n'

    # Test integer
    builder.add_integer(42)
    assert builder.get_response() == b':42\r\n'

    # Test bulk string
    builder.add_bulk(b'hello')
    assert builder.get_response() == b'$5\r\nhello\r\n'

    # Test bulk or null
    builder.add_bulk_or_null(None)
    assert builder.get_response() == RESP_NULL

    builder.add_bulk_or_null(b'test')
    assert builder.get_response() == b'$4\r\ntest\r\n'

    # Test null
    builder.add_null()
    assert builder.get_response() == RESP_NULL

    # Test complex response (array with multiple elements)
    builder.add_array_header(3)
    builder.add_integer(1)
    builder.add_bulk(b'hello')
    builder.add_simple('OK')
    result = builder.get_response()
    expected = b'*3\r\n:1\r\n$5\r\nhello\r\n+OK\r\n'
    assert result == expected

    # Test raw addition
    builder.add_raw(RESP_OK)
    builder.add_raw(RESP_PONG)
    assert builder.get_response() == b'+OK\r\n+PONG\r\n'

    # Test reset
    builder.add_simple('test')
    builder.reset()
    assert len(builder) == 0
    builder.add_simple('OK')
    assert builder.get_response() == b'+OK\r\n'

    print("  [OK] ResponseBuilder working correctly")


def test_memory_efficiency():
    """Test memory-related optimizations."""
    print("Testing memory efficiency...")

    # Test that pre-allocated constants are reused (same object ID)
    assert id(error_wrongtype()) == id(ERR_WRONGTYPE)
    assert id(error_syntax()) == id(ERR_SYNTAX)
    assert id(integer(0)) == id(RESP_ZERO)
    assert id(integer(1)) == id(RESP_ONE)

    # Test ResponseBuilder reuse
    builder = ResponseBuilder()
    builder.add_simple('test1')
    resp1 = builder.get_response()
    builder.add_simple('test2')
    resp2 = builder.get_response()

    assert resp1 == b'+test1\r\n'
    assert resp2 == b'+test2\r\n'

    print("  [OK] Memory optimizations verified")


def run_all_tests():
    """Run all test functions."""
    print("\n" + "="*60)
    print("MicroRedis Response Module Tests")
    print("="*60 + "\n")

    test_preallocated_responses()
    test_preallocated_errors()
    test_simple_string()
    test_error()
    test_integer()
    test_bulk_string()
    test_bulk_string_or_null()
    test_array()
    test_encode_value()
    test_response_builder()
    test_memory_efficiency()

    print("\n" + "="*60)
    print("[OK] All tests passed!")
    print("="*60 + "\n")


if __name__ == '__main__':
    run_all_tests()

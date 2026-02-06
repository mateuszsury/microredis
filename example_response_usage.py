"""
Example usage of microredis/core/response.py

Demonstrates how to build RESP2 responses efficiently for MicroRedis.
"""

from microredis.core.response import (
    # Pre-allocated responses
    RESP_OK, RESP_PONG, RESP_NULL, RESP_ZERO, RESP_ONE,
    # Pre-allocated errors
    ERR_UNKNOWN_CMD, ERR_WRONGTYPE, ERR_SYNTAX,
    # Response builders
    simple_string, error, integer, bulk_string, bulk_string_or_null,
    array, encode_value, ResponseBuilder
)


def example_simple_responses():
    """Example: Using pre-allocated constant responses."""
    print("=== Example 1: Pre-allocated Responses ===")
    print("Most efficient - zero allocation at runtime\n")

    # Simulate Redis PING command
    response = RESP_OK
    print(f"PING response: {response}")

    # Simulate GET on non-existent key
    response = RESP_NULL
    print(f"GET (not found): {response}")

    # Simulate INCR starting from 0
    response = RESP_ONE
    print(f"INCR result: {response}\n")


def example_dynamic_responses():
    """Example: Building dynamic responses."""
    print("=== Example 2: Dynamic Response Building ===")
    print("For responses that vary by content\n")

    # Simple string response
    response = simple_string("Server ready")
    print(f"INFO response: {response}")

    # Error response
    response = error("key does not exist")
    print(f"Custom error: {response}")

    # Integer response
    response = integer(12345)
    print(f"INCR counter: {response}")

    # Bulk string response
    response = bulk_string(b"Hello, Redis!")
    print(f"GET value: {response}\n")


def example_array_responses():
    """Example: Building array responses."""
    print("=== Example 3: Array Responses ===")
    print("For commands returning multiple values\n")

    # KEYS command - returns array of key names
    keys = [b"user:1", b"user:2", b"user:3"]
    encoded_keys = [bulk_string(key) for key in keys]
    response = array(encoded_keys)
    print(f"KEYS response:\n{response}")

    # Using encode_value for automatic encoding
    response = encode_value(["key1", "key2", "key3"])
    print(f"Auto-encoded array:\n{response}\n")


def example_response_builder():
    """Example: Using ResponseBuilder for complex responses."""
    print("=== Example 4: ResponseBuilder Class ===")
    print("Most efficient for multi-part responses\n")

    builder = ResponseBuilder()

    # Example: HGETALL response (hash with key-value pairs)
    builder.add_array_header(6)  # 3 fields * 2 (key + value)
    builder.add_bulk(b"name")
    builder.add_bulk(b"John Doe")
    builder.add_bulk(b"age")
    builder.add_bulk(b"30")
    builder.add_bulk(b"city")
    builder.add_bulk(b"Warsaw")

    response = builder.get_response()
    print(f"HGETALL response:\n{response}")

    # Builder is automatically reset after get_response()
    # Can be reused for next response

    # Example: LRANGE response (list elements)
    builder.add_array_header(4)
    builder.add_bulk(b"item1")
    builder.add_bulk(b"item2")
    builder.add_bulk(b"item3")
    builder.add_bulk(b"item4")

    response = builder.get_response()
    print(f"LRANGE response:\n{response}\n")


def example_mixed_types():
    """Example: Arrays with mixed element types."""
    print("=== Example 5: Mixed Type Arrays ===")
    print("Using encode_value for heterogeneous data\n")

    # EXEC command result (transaction results)
    results = [
        "OK",           # SET result
        1,              # INCR result
        None,           # GET non-existent key
        ["a", "b", "c"] # LRANGE result
    ]

    response = encode_value(results)
    print(f"EXEC (transaction) response:\n{response}\n")


def example_error_responses():
    """Example: Error handling."""
    print("=== Example 6: Error Responses ===")
    print("Pre-allocated errors for common cases\n")

    # Unknown command
    response = ERR_UNKNOWN_CMD
    print(f"Unknown command: {response}")

    # Wrong type operation
    response = ERR_WRONGTYPE
    print(f"Type error: {response}")

    # Syntax error
    response = ERR_SYNTAX
    print(f"Syntax error: {response}")

    # Custom error
    response = error("index out of range")
    print(f"Custom error: {response}\n")


def example_null_handling():
    """Example: NULL values in responses."""
    print("=== Example 7: NULL Value Handling ===")
    print("Different ways to represent absence of data\n")

    # GET on non-existent key
    response = RESP_NULL
    print(f"GET (not found): {response}")

    # Using bulk_string_or_null
    response = bulk_string_or_null(None)
    print(f"Conditional NULL: {response}")

    response = bulk_string_or_null(b"found")
    print(f"Conditional value: {response}\n")


def example_performance_tips():
    """Example: Performance optimization tips."""
    print("=== Example 8: Performance Tips ===\n")

    print("1. ALWAYS prefer pre-allocated constants:")
    print("   GOOD: return RESP_OK")
    print("   BAD:  return simple_string('OK')\n")

    print("2. For integers 0 and 1, use pre-allocated:")
    print("   GOOD: return RESP_ZERO")
    print("   BAD:  return integer(0)\n")

    print("3. Use ResponseBuilder for multi-part responses:")
    print("   GOOD: builder.add_bulk(...)  # Builds in single buffer")
    print("   BAD:  response += bulk_string(...)  # Creates new bytes each time\n")

    print("4. Reuse ResponseBuilder instances:")
    builder = ResponseBuilder()
    print("   builder = ResponseBuilder()  # Create once")
    print("   builder.add_simple('OK')")
    print("   resp = builder.get_response()  # Auto-resets for reuse")
    print("   builder.add_error('err')  # Reuse same builder\n")

    print("5. Use encode_value() for dynamic type detection:")
    print("   response = encode_value([1, 'test', None])  # Auto-detects types\n")


def run_all_examples():
    """Run all example demonstrations."""
    print("\n" + "="*70)
    print("MicroRedis Response Module - Usage Examples")
    print("="*70 + "\n")

    example_simple_responses()
    example_dynamic_responses()
    example_array_responses()
    example_response_builder()
    example_mixed_types()
    example_error_responses()
    example_null_handling()
    example_performance_tips()

    print("="*70)
    print("Examples completed!")
    print("="*70 + "\n")


if __name__ == '__main__':
    run_all_examples()

"""
Test importing from microredis.core package
"""

print("Testing imports from microredis.core...\n")

# Test importing from core package directly
from microredis.core import (
    RESP_OK, RESP_PONG, RESP_NULL,
    ERR_WRONGTYPE, ERR_SYNTAX,
    simple_string, integer, bulk_string,
    ResponseBuilder,
    DEFAULT_PORT, MAX_CLIENTS, CRLF
)

print("[OK] Successfully imported from microredis.core")

# Test using imported items
print("\nTesting imported items:")
print(f"  RESP_OK: {RESP_OK}")
print(f"  DEFAULT_PORT: {DEFAULT_PORT}")
print(f"  simple_string('test'): {simple_string('test')}")

builder = ResponseBuilder()
builder.add_simple('OK')
response = builder.get_response()
print(f"  ResponseBuilder test: {response}")

print("\n[OK] All imports working correctly!")

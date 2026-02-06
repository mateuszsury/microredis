"""
Integration example: Using response.py in a Redis command handler

This demonstrates how the response module would be used in the actual
MicroRedis server implementation to handle Redis commands.
"""

from microredis.core import (
    RESP_OK, RESP_PONG, RESP_NULL, RESP_ZERO, RESP_ONE,
    ERR_WRONG_ARITY, ERR_WRONGTYPE, ERR_UNKNOWN_CMD,
    integer, bulk_string, bulk_string_or_null, array, encode_value,
    ResponseBuilder
)


# Simulated storage backend
class SimpleStorage:
    """Mock storage for demonstration purposes."""

    def __init__(self):
        self._data = {}
        self._lists = {}
        self._hashes = {}

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value):
        self._data[key] = value

    def exists(self, key):
        return key in self._data

    def delete(self, key):
        if key in self._data:
            del self._data[key]
            return True
        return False

    def incr(self, key):
        value = self._data.get(key, b'0')
        try:
            num = int(value)
            num += 1
            self._data[key] = str(num).encode()
            return num
        except ValueError:
            return None

    def keys(self):
        return list(self._data.keys())

    def lpush(self, key, *values):
        if key not in self._lists:
            self._lists[key] = []
        self._lists[key] = list(reversed(values)) + self._lists[key]
        return len(self._lists[key])

    def lrange(self, key, start, stop):
        if key not in self._lists:
            return []
        return self._lists[key][start:stop+1]

    def hset(self, key, field, value):
        if key not in self._hashes:
            self._hashes[key] = {}
        self._hashes[key][field] = value

    def hgetall(self, key):
        return self._hashes.get(key, {})


# Global storage instance
storage = SimpleStorage()


# Command handlers
class RedisCommandHandler:
    """Handles Redis commands and returns RESP2 responses."""

    def __init__(self, storage):
        self.storage = storage
        # Reusable response builder for complex responses
        self.builder = ResponseBuilder(initial_capacity=512)

    def handle_command(self, command, args):
        """
        Dispatch command to appropriate handler.

        Args:
            command: Command name (uppercase)
            args: List of command arguments (bytes)

        Returns:
            RESP2-encoded response (bytes)
        """
        handler = getattr(self, f'cmd_{command.lower()}', None)
        if handler is None:
            return ERR_UNKNOWN_CMD

        return handler(args)

    # =========================================================================
    # String Commands
    # =========================================================================

    def cmd_ping(self, args):
        """PING [message] - Ping the server."""
        if len(args) == 0:
            return RESP_PONG
        elif len(args) == 1:
            return bulk_string(args[0])
        else:
            return ERR_WRONG_ARITY

    def cmd_get(self, args):
        """GET key - Get value of key."""
        if len(args) != 1:
            return ERR_WRONG_ARITY

        key = args[0]
        value = self.storage.get(key)
        return bulk_string_or_null(value)

    def cmd_set(self, args):
        """SET key value - Set key to hold value."""
        if len(args) != 2:
            return ERR_WRONG_ARITY

        key, value = args
        self.storage.set(key, value)
        return RESP_OK

    def cmd_exists(self, args):
        """EXISTS key - Check if key exists."""
        if len(args) != 1:
            return ERR_WRONG_ARITY

        key = args[0]
        exists = self.storage.exists(key)
        return RESP_ONE if exists else RESP_ZERO

    def cmd_del(self, args):
        """DEL key - Delete a key."""
        if len(args) != 1:
            return ERR_WRONG_ARITY

        key = args[0]
        deleted = self.storage.delete(key)
        return RESP_ONE if deleted else RESP_ZERO

    def cmd_incr(self, args):
        """INCR key - Increment integer value of key."""
        if len(args) != 1:
            return ERR_WRONG_ARITY

        key = args[0]
        result = self.storage.incr(key)

        if result is None:
            return ERR_WRONGTYPE

        return integer(result)

    def cmd_keys(self, args):
        """KEYS pattern - Get all keys (simplified - no pattern matching)."""
        if len(args) != 1:
            return ERR_WRONG_ARITY

        keys = self.storage.keys()
        if not keys:
            return encode_value([])

        # Use ResponseBuilder for efficiency
        self.builder.add_array_header(len(keys))
        for key in keys:
            self.builder.add_bulk(key)

        return self.builder.get_response()

    # =========================================================================
    # List Commands
    # =========================================================================

    def cmd_lpush(self, args):
        """LPUSH key value [value ...] - Prepend values to list."""
        if len(args) < 2:
            return ERR_WRONG_ARITY

        key = args[0]
        values = args[1:]
        length = self.storage.lpush(key, *values)

        return integer(length)

    def cmd_lrange(self, args):
        """LRANGE key start stop - Get range of elements from list."""
        if len(args) != 3:
            return ERR_WRONG_ARITY

        key = args[0]
        try:
            start = int(args[1])
            stop = int(args[2])
        except ValueError:
            return ERR_WRONGTYPE

        elements = self.storage.lrange(key, start, stop)

        # Use ResponseBuilder for array response
        self.builder.add_array_header(len(elements))
        for element in elements:
            self.builder.add_bulk(element)

        return self.builder.get_response()

    # =========================================================================
    # Hash Commands
    # =========================================================================

    def cmd_hset(self, args):
        """HSET key field value - Set hash field to value."""
        if len(args) != 3:
            return ERR_WRONG_ARITY

        key, field, value = args
        self.storage.hset(key, field, value)
        return RESP_ONE

    def cmd_hgetall(self, args):
        """HGETALL key - Get all fields and values from hash."""
        if len(args) != 1:
            return ERR_WRONG_ARITY

        key = args[0]
        hash_data = self.storage.hgetall(key)

        if not hash_data:
            return encode_value([])

        # Use ResponseBuilder for hash response
        self.builder.add_array_header(len(hash_data) * 2)
        for field, value in hash_data.items():
            self.builder.add_bulk(field)
            self.builder.add_bulk(value)

        return self.builder.get_response()


# Demonstration
def demonstrate_handlers():
    """Demonstrate command handlers in action."""
    print("="*70)
    print("MicroRedis Integration Example - Command Handlers")
    print("="*70 + "\n")

    handler = RedisCommandHandler(storage)

    # Test cases
    commands = [
        ("PING", []),
        ("SET", [b"mykey", b"myvalue"]),
        ("GET", [b"mykey"]),
        ("GET", [b"nonexistent"]),
        ("EXISTS", [b"mykey"]),
        ("INCR", [b"counter"]),
        ("INCR", [b"counter"]),
        ("INCR", [b"counter"]),
        ("KEYS", [b"*"]),
        ("LPUSH", [b"mylist", b"item1", b"item2", b"item3"]),
        ("LRANGE", [b"mylist", b"0", b"-1"]),
        ("HSET", [b"user:1", b"name", b"John"]),
        ("HSET", [b"user:1", b"age", b"30"]),
        ("HGETALL", [b"user:1"]),
        ("DEL", [b"mykey"]),
        ("GET", [b"mykey"]),
        ("UNKNOWN", [b"arg"]),  # Unknown command
    ]

    for cmd, args in commands:
        response = handler.handle_command(cmd, args)
        args_str = ' '.join(arg.decode('utf-8') if isinstance(arg, bytes) else str(arg) for arg in args)
        print(f"Command: {cmd} {args_str}")
        print(f"Response: {response}")
        print()

    print("="*70)
    print("Integration example completed successfully!")
    print("="*70)


if __name__ == '__main__':
    demonstrate_handlers()

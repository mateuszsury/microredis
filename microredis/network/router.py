"""Command router for MicroRedis.

Provides command dispatch table with arity validation, flags, and key position metadata.
Optimized for O(1) command lookup on MicroPython/ESP32-S3.
"""

from microredis.core.response import (
    error, simple_string, integer, bulk_string, bulk_string_or_null,
    array, encode_value, ResponseBuilder,
    RESP_OK, RESP_PONG, RESP_NULL, RESP_ZERO, RESP_ONE
)
from microredis.storage.engine import Storage
from microredis.exceptions import RedisError
from microredis.storage.datatypes import (
    StringOperations, HashType, ListOperations, SetOperations, SortedSetOperations
)
from microredis.commands.hyperloglog import HyperLogLog
from microredis.commands.bitmaps import BitmapOperations
from microredis.commands.streams import StreamOperations


class CommandInfo:
    """Metadata about a Redis command.

    Attributes:
        name: Command name as bytes
        handler: Callable that executes the command
        arity: Argument count (>0 exact, <0 minimum, 0 any)
        flags: Command flags (readonly, write, fast, slow, etc.)
        first_key: Position of first key argument (0 if no keys)
        last_key: Position of last key argument
        step: Step between key arguments
    """
    __slots__ = ('name', 'handler', 'arity', 'flags', 'first_key', 'last_key', 'step')

    def __init__(self, name, handler, arity, flags=(), first_key=0, last_key=0, step=0):
        self.name = name
        self.handler = handler
        self.arity = arity
        self.flags = flags
        self.first_key = first_key
        self.last_key = last_key
        self.step = step


class CommandRouter:
    """Command dispatch table for MicroRedis.

    Routes commands to handlers with arity validation and metadata tracking.
    Uses dict for O(1) command lookup.
    """
    __slots__ = ('_commands', '_storage', '_categories')

    def __init__(self, storage):
        """Initialize router with storage engine.

        Args:
            storage: Storage engine instance
        """
        self._storage = storage
        self._commands = {}
        self._categories = {
            'connection': [],
            'string': [],
            'keys': [],
            'hash': [],
            'list': [],
            'set': [],
            'sortedset': []
        }
        self._register_all_commands()

    def register(self, name, handler, arity, flags=(), first_key=0, last_key=0, step=0):
        """Register a command with the router.

        Args:
            name: Command name as bytes
            handler: Callable(storage, *args) -> bytes
            arity: Argument count including command name
            flags: Tuple of flag strings
            first_key: Position of first key (1-indexed, 0 if none)
            last_key: Position of last key
            step: Step between keys
        """
        cmd_upper = name.upper() if isinstance(name, bytes) else name.encode().upper()
        cmd_info = CommandInfo(cmd_upper, handler, arity, flags, first_key, last_key, step)
        self._commands[cmd_upper] = cmd_info

        # Categorize command
        for category, commands in self._categories.items():
            if category in flags:
                commands.append(cmd_upper)
                break

    def execute(self, cmd, args):
        """Execute a command with arguments.

        Args:
            cmd: Command name as bytes
            args: List of argument bytes (excluding command name)

        Returns:
            RESP2-encoded response bytes
        """
        cmd_upper = cmd.upper() if isinstance(cmd, bytes) else cmd.encode().upper()
        cmd_info = self._commands.get(cmd_upper)

        if cmd_info is None:
            return error(f"ERR unknown command '{cmd.decode() if isinstance(cmd, bytes) else cmd}'")

        # Arity validation (arity includes command name, args doesn't)
        if not self._check_arity(cmd_info, args):
            return error(f"ERR wrong number of arguments for '{cmd.decode() if isinstance(cmd, bytes) else cmd}' command")

        try:
            return cmd_info.handler(self._storage, *args)
        except RedisError as e:
            return e.to_resp()
        except TypeError as e:
            msg = str(e)
            if msg.startswith('WRONGTYPE'):
                return error(msg)
            return error(f"ERR {msg}")
        except MemoryError as e:
            msg = str(e)
            if msg.startswith('OOM'):
                return error(msg)
            return error(f"ERR {msg}")
        except KeyError as e:
            return error(f"ERR {str(e)}")
        except (ValueError, OverflowError) as e:
            return error(f"ERR {str(e)}")

    def _check_arity(self, cmd_info, args):
        """Check if argument count matches command arity.

        Args:
            cmd_info: CommandInfo instance
            args: List of arguments (excluding command name)

        Returns:
            True if arity is valid
        """
        arg_count = len(args) + 1  # +1 for command name

        if cmd_info.arity > 0:
            # Exact argument count
            return arg_count == cmd_info.arity
        elif cmd_info.arity < 0:
            # Minimum argument count
            return arg_count >= abs(cmd_info.arity)
        else:
            # Any number of arguments
            return True

    def get_command_info(self, cmd):
        """Get command metadata.

        Args:
            cmd: Command name as bytes or str

        Returns:
            CommandInfo instance or None
        """
        cmd_upper = cmd.upper() if isinstance(cmd, bytes) else cmd.encode().upper()
        return self._commands.get(cmd_upper)

    def get_commands(self):
        """Get all registered commands.

        Returns:
            Dict of command name -> CommandInfo
        """
        return self._commands.copy()

    def get_commands_by_category(self, category):
        """Get commands by category.

        Args:
            category: Category name (connection, string, keys, etc.)

        Returns:
            List of command names as bytes
        """
        return self._categories.get(category, []).copy()

    def _register_all_commands(self):
        """Register all Phase 1 and 2 commands."""
        # Connection commands
        self.register(b'PING', self._cmd_ping, -1, ('connection', 'fast'), 0, 0, 0)
        self.register(b'ECHO', self._cmd_echo, 2, ('connection', 'fast'), 0, 0, 0)
        self.register(b'QUIT', self._cmd_quit, 1, ('connection', 'fast'), 0, 0, 0)
        self.register(b'COMMAND', self._cmd_command, 1, ('connection', 'slow'), 0, 0, 0)
        self.register(b'INFO', self._cmd_info, -1, ('connection', 'slow'), 0, 0, 0)

        # String commands
        self.register(b'GET', self._cmd_get, 2, ('string', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'SET', self._cmd_set, -3, ('string', 'write', 'fast'), 1, 1, 1)
        self.register(b'APPEND', self._cmd_append, 3, ('string', 'write', 'fast'), 1, 1, 1)
        self.register(b'STRLEN', self._cmd_strlen, 2, ('string', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'INCR', self._cmd_incr, 2, ('string', 'write', 'fast'), 1, 1, 1)
        self.register(b'DECR', self._cmd_decr, 2, ('string', 'write', 'fast'), 1, 1, 1)
        self.register(b'INCRBY', self._cmd_incrby, 3, ('string', 'write', 'fast'), 1, 1, 1)
        self.register(b'DECRBY', self._cmd_decrby, 3, ('string', 'write', 'fast'), 1, 1, 1)
        self.register(b'INCRBYFLOAT', self._cmd_incrbyfloat, 3, ('string', 'write', 'fast'), 1, 1, 1)
        self.register(b'GETRANGE', self._cmd_getrange, 4, ('string', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'SETRANGE', self._cmd_setrange, 4, ('string', 'write', 'fast'), 1, 1, 1)
        self.register(b'SETBIT', self._cmd_setbit, 4, ('string', 'write', 'fast'), 1, 1, 1)
        self.register(b'GETBIT', self._cmd_getbit, 3, ('string', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'BITCOUNT', self._cmd_bitcount, -2, ('string', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'MGET', self._cmd_mget, -2, ('string', 'readonly', 'fast'), 1, -1, 1)
        self.register(b'MSET', self._cmd_mset, -3, ('string', 'write', 'fast'), 1, -1, 2)
        self.register(b'MSETNX', self._cmd_msetnx, -3, ('string', 'write', 'fast'), 1, -1, 2)

        # Additional string commands (P0 priority)
        self.register(b'SETNX', self._cmd_setnx, 3, ('string', 'write', 'fast'), 1, 1, 1)
        self.register(b'SETEX', self._cmd_setex, 4, ('string', 'write', 'fast'), 1, 1, 1)
        self.register(b'PSETEX', self._cmd_psetex, 4, ('string', 'write', 'fast'), 1, 1, 1)
        self.register(b'GETSET', self._cmd_getset, 3, ('string', 'write', 'fast'), 1, 1, 1)
        self.register(b'GETDEL', self._cmd_getdel, 2, ('string', 'write', 'fast'), 1, 1, 1)
        self.register(b'GETEX', self._cmd_getex, -2, ('string', 'write', 'fast'), 1, 1, 1)

        # Key commands
        self.register(b'DEL', self._cmd_del, -2, ('keys', 'write', 'fast'), 1, -1, 1)
        self.register(b'EXISTS', self._cmd_exists, -2, ('keys', 'readonly', 'fast'), 1, -1, 1)
        self.register(b'TYPE', self._cmd_type, 2, ('keys', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'KEYS', self._cmd_keys, 2, ('keys', 'readonly', 'slow'), 0, 0, 0)
        self.register(b'RENAME', self._cmd_rename, 3, ('keys', 'write', 'fast'), 1, 2, 1)
        self.register(b'RENAMENX', self._cmd_renamenx, 3, ('keys', 'write', 'fast'), 1, 2, 1)
        self.register(b'TTL', self._cmd_ttl, 2, ('keys', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'PTTL', self._cmd_pttl, 2, ('keys', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'EXPIRE', self._cmd_expire, 3, ('keys', 'write', 'fast'), 1, 1, 1)
        self.register(b'EXPIREAT', self._cmd_expireat, 3, ('keys', 'write', 'fast'), 1, 1, 1)
        self.register(b'PEXPIRE', self._cmd_pexpire, 3, ('keys', 'write', 'fast'), 1, 1, 1)
        self.register(b'PEXPIREAT', self._cmd_pexpireat, 3, ('keys', 'write', 'fast'), 1, 1, 1)
        self.register(b'PERSIST', self._cmd_persist, 2, ('keys', 'write', 'fast'), 1, 1, 1)

        # Hash commands
        self.register(b'HSET', self._cmd_hset, -4, ('hash', 'write', 'fast'), 1, 1, 1)
        self.register(b'HGET', self._cmd_hget, 3, ('hash', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'HDEL', self._cmd_hdel, -3, ('hash', 'write', 'fast'), 1, 1, 1)
        self.register(b'HEXISTS', self._cmd_hexists, 3, ('hash', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'HGETALL', self._cmd_hgetall, 2, ('hash', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'HKEYS', self._cmd_hkeys, 2, ('hash', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'HVALS', self._cmd_hvals, 2, ('hash', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'HLEN', self._cmd_hlen, 2, ('hash', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'HMGET', self._cmd_hmget, -3, ('hash', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'HINCRBY', self._cmd_hincrby, 4, ('hash', 'write', 'fast'), 1, 1, 1)
        self.register(b'HINCRBYFLOAT', self._cmd_hincrbyfloat, 4, ('hash', 'write', 'fast'), 1, 1, 1)

        # List commands
        self.register(b'LPUSH', self._cmd_lpush, -3, ('list', 'write', 'fast'), 1, 1, 1)
        self.register(b'RPUSH', self._cmd_rpush, -3, ('list', 'write', 'fast'), 1, 1, 1)
        self.register(b'LPOP', self._cmd_lpop, -2, ('list', 'write', 'fast'), 1, 1, 1)
        self.register(b'RPOP', self._cmd_rpop, -2, ('list', 'write', 'fast'), 1, 1, 1)
        self.register(b'LLEN', self._cmd_llen, 2, ('list', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'LRANGE', self._cmd_lrange, 4, ('list', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'LINDEX', self._cmd_lindex, 3, ('list', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'LSET', self._cmd_lset, 4, ('list', 'write', 'fast'), 1, 1, 1)
        self.register(b'LTRIM', self._cmd_ltrim, 4, ('list', 'write', 'fast'), 1, 1, 1)

        # Set commands
        self.register(b'SADD', self._cmd_sadd, -3, ('set', 'write', 'fast'), 1, 1, 1)
        self.register(b'SREM', self._cmd_srem, -3, ('set', 'write', 'fast'), 1, 1, 1)
        self.register(b'SISMEMBER', self._cmd_sismember, 3, ('set', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'SMEMBERS', self._cmd_smembers, 2, ('set', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'SCARD', self._cmd_scard, 2, ('set', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'SINTER', self._cmd_sinter, -2, ('set', 'readonly', 'slow'), 1, -1, 1)
        self.register(b'SUNION', self._cmd_sunion, -2, ('set', 'readonly', 'slow'), 1, -1, 1)
        self.register(b'SDIFF', self._cmd_sdiff, -2, ('set', 'readonly', 'slow'), 1, -1, 1)
        self.register(b'SINTERSTORE', self._cmd_sinterstore, -3, ('set', 'write', 'slow'), 1, -1, 1)
        self.register(b'SUNIONSTORE', self._cmd_sunionstore, -3, ('set', 'write', 'slow'), 1, -1, 1)
        self.register(b'SDIFFSTORE', self._cmd_sdiffstore, -3, ('set', 'write', 'slow'), 1, -1, 1)

        # Sorted Set commands
        self.register(b'ZADD', self._cmd_zadd, -4, ('sortedset', 'write', 'fast'), 1, 1, 1)
        self.register(b'ZREM', self._cmd_zrem, -3, ('sortedset', 'write', 'fast'), 1, 1, 1)
        self.register(b'ZSCORE', self._cmd_zscore, 3, ('sortedset', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'ZRANK', self._cmd_zrank, 3, ('sortedset', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'ZREVRANK', self._cmd_zrevrank, 3, ('sortedset', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'ZCARD', self._cmd_zcard, 2, ('sortedset', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'ZRANGE', self._cmd_zrange, -4, ('sortedset', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'ZREVRANGE', self._cmd_zrevrange, -4, ('sortedset', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'ZINCRBY', self._cmd_zincrby, 4, ('sortedset', 'write', 'fast'), 1, 1, 1)
        self.register(b'ZCOUNT', self._cmd_zcount, 4, ('sortedset', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'ZRANGEBYSCORE', self._cmd_zrangebyscore, -4, ('sortedset', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'ZREVRANGEBYSCORE', self._cmd_zrevrangebyscore, -4, ('sortedset', 'readonly', 'slow'), 1, 1, 1)

        # HyperLogLog commands
        self.register(b'PFADD', self._cmd_pfadd, -2, ('hyperloglog', 'write', 'fast'), 1, 1, 1)
        self.register(b'PFCOUNT', self._cmd_pfcount, -2, ('hyperloglog', 'readonly', 'slow'), 1, -1, 1)
        self.register(b'PFMERGE', self._cmd_pfmerge, -3, ('hyperloglog', 'write', 'slow'), 1, -1, 1)

        # Bitmap commands (additional to string commands)
        self.register(b'BITPOS', self._cmd_bitpos, -3, ('bitmap', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'BITOP', self._cmd_bitop, -4, ('bitmap', 'write', 'slow'), 2, -1, 1)
        self.register(b'BITFIELD', self._cmd_bitfield, -2, ('bitmap', 'write', 'slow'), 1, 1, 1)

        # Stream commands
        self.register(b'XADD', self._cmd_xadd, -5, ('stream', 'write', 'fast'), 1, 1, 1)
        self.register(b'XLEN', self._cmd_xlen, 2, ('stream', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'XRANGE', self._cmd_xrange, -4, ('stream', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'XREVRANGE', self._cmd_xrevrange, -4, ('stream', 'readonly', 'slow'), 1, 1, 1)
        self.register(b'XREAD', self._cmd_xread, -4, ('stream', 'readonly', 'slow'), 0, 0, 0)
        self.register(b'XTRIM', self._cmd_xtrim, -4, ('stream', 'write', 'slow'), 1, 1, 1)

        # Set operations (additional)
        self.register(b'SPOP', self._cmd_spop, -2, ('set', 'write', 'fast'), 1, 1, 1)
        self.register(b'SRANDMEMBER', self._cmd_srandmember, -2, ('set', 'readonly', 'fast'), 1, 1, 1)
        self.register(b'SMOVE', self._cmd_smove, 4, ('set', 'write', 'fast'), 1, 2, 1)

        # Server commands
        self.register(b'DBSIZE', self._cmd_dbsize, 1, ('connection', 'readonly', 'fast'), 0, 0, 0)
        self.register(b'FLUSHDB', self._cmd_flushdb, -1, ('connection', 'write', 'slow'), 0, 0, 0)
        self.register(b'TIME', self._cmd_time, 1, ('connection', 'readonly', 'fast'), 0, 0, 0)

    # Connection command handlers

    @staticmethod
    def _cmd_ping(storage, *args):
        """PING [message] - returns PONG or echoes message."""
        if len(args) == 0:
            return RESP_PONG
        return bulk_string(args[0])

    @staticmethod
    def _cmd_echo(storage, *args):
        """ECHO message - returns message."""
        return bulk_string(args[0])

    @staticmethod
    def _cmd_quit(storage, *args):
        """QUIT - returns OK (connection close handled externally)."""
        return RESP_OK

    def _cmd_command(self, storage, *args):
        """COMMAND - returns command metadata array."""
        builder = ResponseBuilder()
        builder.add_array_header(len(self._commands))

        for cmd_info in self._commands.values():
            # Each command is an array: [name, arity, flags, first_key, last_key, step]
            cmd_array = [
                bulk_string(cmd_info.name),
                integer(cmd_info.arity),
                array([bulk_string(f.encode()) for f in cmd_info.flags]),
                integer(cmd_info.first_key),
                integer(cmd_info.last_key),
                integer(cmd_info.step)
            ]
            # Use add_raw with pre-encoded array (add_array doesn't exist)
            builder.add_raw(array(cmd_array))

        return builder.get_response()

    @staticmethod
    def _cmd_info(storage, *args):
        """INFO [section] - returns server info."""
        # Simplified INFO response
        info_lines = [
            b'# Server',
            b'microredis_version:0.1.0',
            b'platform:micropython',
            b'',
            b'# Memory',
            b'used_memory:0',
            b'',
            b'# Stats',
            b'total_connections_received:0',
            b'total_commands_processed:0'
        ]
        return bulk_string(b'\r\n'.join(info_lines))

    # String command handlers

    @staticmethod
    def _cmd_get(storage, *args):
        """GET key - retrieves value."""
        return bulk_string_or_null(storage.get(args[0]))

    @staticmethod
    def _cmd_set(storage, *args):
        """SET key value [EX seconds] [PX ms] [NX|XX] - stores with options."""
        key, value = args[0], args[1]
        ex, px, nx, xx = None, None, False, False

        # Parse options
        i = 2
        while i < len(args):
            opt = args[i].upper()
            if opt == b'EX' and i + 1 < len(args):
                ex = int(args[i + 1])
                i += 2
            elif opt == b'PX' and i + 1 < len(args):
                px = int(args[i + 1])
                i += 2
            elif opt == b'NX':
                nx = True
                i += 1
            elif opt == b'XX':
                xx = True
                i += 1
            else:
                return error("syntax error")

        # NX and XX are mutually exclusive
        if nx and xx:
            return error("ERR XX and NX options at the same time are not compatible")

        if storage.set(key, value, ex=ex, px=px, nx=nx, xx=xx):
            return RESP_OK
        return RESP_NULL

    @staticmethod
    def _cmd_append(storage, *args):
        """APPEND key value - append to string."""
        new_len = StringOperations.append(storage, args[0], args[1])
        return integer(new_len)

    @staticmethod
    def _cmd_strlen(storage, *args):
        """STRLEN key - get string length."""
        length = StringOperations.strlen(storage, args[0])
        return integer(length)

    @staticmethod
    def _cmd_incr(storage, *args):
        """INCR key - increment by 1."""
        try:
            result = StringOperations.incr(storage, args[0])
            return integer(result)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_decr(storage, *args):
        """DECR key - decrement by 1."""
        try:
            result = StringOperations.decr(storage, args[0])
            return integer(result)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_incrby(storage, *args):
        """INCRBY key increment - increment by amount."""
        try:
            increment = int(args[1])
            result = StringOperations.incrby(storage, args[0], increment)
            return integer(result)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_decrby(storage, *args):
        """DECRBY key decrement - decrement by amount."""
        try:
            decrement = int(args[1])
            result = StringOperations.decrby(storage, args[0], decrement)
            return integer(result)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_incrbyfloat(storage, *args):
        """INCRBYFLOAT key increment - float increment."""
        try:
            increment = float(args[1])
            result = StringOperations.incrbyfloat(storage, args[0], increment)
            return bulk_string(result)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_getrange(storage, *args):
        """GETRANGE key start end - substring."""
        try:
            start = int(args[1])
            end = int(args[2])
            result = StringOperations.getrange(storage, args[0], start, end)
            return bulk_string(result)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_setrange(storage, *args):
        """SETRANGE key offset value - overwrite at offset."""
        try:
            offset = int(args[1])
            new_len = StringOperations.setrange(storage, args[0], offset, args[2])
            return integer(new_len)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_setbit(storage, *args):
        """SETBIT key offset value - set bit."""
        try:
            offset = int(args[1])
            value = int(args[2])
            old_value = StringOperations.setbit(storage, args[0], offset, value)
            return integer(old_value)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_getbit(storage, *args):
        """GETBIT key offset - get bit."""
        try:
            offset = int(args[1])
            value = StringOperations.getbit(storage, args[0], offset)
            return integer(value)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_bitcount(storage, *args):
        """BITCOUNT key [start end] - count set bits."""
        try:
            if len(args) == 1:
                count = StringOperations.bitcount(storage, args[0])
            else:
                start = int(args[1])
                end = int(args[2])
                count = StringOperations.bitcount(storage, args[0], start, end)
            return integer(count)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_mget(storage, *args):
        """MGET key [key...] - get multiple values."""
        values = StringOperations.mget(storage, *args)
        return array([bulk_string_or_null(v) for v in values])

    @staticmethod
    def _cmd_mset(storage, *args):
        """MSET key value [key value...] - set multiple."""
        if len(args) % 2 != 0:
            return error("wrong number of arguments for 'mset' command")

        mapping = {args[i]: args[i + 1] for i in range(0, len(args), 2)}
        StringOperations.mset(storage, mapping)
        return RESP_OK

    @staticmethod
    def _cmd_msetnx(storage, *args):
        """MSETNX key value [key value...] - set multiple only if none exist."""
        if len(args) % 2 != 0:
            return error("wrong number of arguments for 'msetnx' command")

        mapping = {args[i]: args[i + 1] for i in range(0, len(args), 2)}
        result = StringOperations.msetnx(storage, mapping)
        return RESP_ONE if result else RESP_ZERO

    @staticmethod
    def _cmd_setnx(storage, *args):
        """SETNX key value - set key only if it doesn't exist."""
        result = StringOperations.setnx(storage, args[0], args[1])
        return RESP_ONE if result else RESP_ZERO

    @staticmethod
    def _cmd_setex(storage, *args):
        """SETEX key seconds value - set with expiration in seconds."""
        try:
            seconds = int(args[1])
            StringOperations.setex(storage, args[0], seconds, args[2])
            return RESP_OK
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_psetex(storage, *args):
        """PSETEX key milliseconds value - set with expiration in milliseconds."""
        try:
            milliseconds = int(args[1])
            StringOperations.psetex(storage, args[0], milliseconds, args[2])
            return RESP_OK
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_getset(storage, *args):
        """GETSET key value - set and return old value."""
        old_value = StringOperations.getset(storage, args[0], args[1])
        return bulk_string_or_null(old_value)

    @staticmethod
    def _cmd_getdel(storage, *args):
        """GETDEL key - get value and delete key."""
        value = StringOperations.getdel(storage, args[0])
        return bulk_string_or_null(value)

    @staticmethod
    def _cmd_getex(storage, *args):
        """GETEX key [EX seconds | PX ms | EXAT ts | PXAT ms-ts | PERSIST] - get with expiry option."""
        key = args[0]
        ex, px, exat, pxat, persist = None, None, None, None, False

        i = 1
        while i < len(args):
            opt = args[i].upper()
            if opt == b'EX' and i + 1 < len(args):
                ex = int(args[i + 1])
                i += 2
            elif opt == b'PX' and i + 1 < len(args):
                px = int(args[i + 1])
                i += 2
            elif opt == b'EXAT' and i + 1 < len(args):
                exat = int(args[i + 1])
                i += 2
            elif opt == b'PXAT' and i + 1 < len(args):
                pxat = int(args[i + 1])
                i += 2
            elif opt == b'PERSIST':
                persist = True
                i += 1
            else:
                return error("syntax error")

        value = StringOperations.getex(storage, key, ex=ex, px=px, exat=exat, pxat=pxat, persist=persist)
        return bulk_string_or_null(value)

    # Key command handlers

    @staticmethod
    def _cmd_del(storage, *args):
        """DEL key [key...] - delete keys."""
        count = storage.delete(*args)
        return integer(count)

    @staticmethod
    def _cmd_exists(storage, *args):
        """EXISTS key [key...] - check existence."""
        count = storage.exists(*args)
        return integer(count)

    @staticmethod
    def _cmd_type(storage, *args):
        """TYPE key - get key type."""
        key_type = storage.type(args[0])
        return simple_string(key_type)

    @staticmethod
    def _cmd_keys(storage, *args):
        """KEYS pattern - find keys by pattern."""
        keys = storage.keys(args[0])
        return array([bulk_string(k) for k in keys])

    @staticmethod
    def _cmd_rename(storage, *args):
        """RENAME key newkey - rename key."""
        try:
            storage.rename(args[0], args[1])
            return RESP_OK
        except Exception as e:
            return error(str(e))

    @staticmethod
    def _cmd_renamenx(storage, *args):
        """RENAMENX key newkey - rename only if newkey doesn't exist."""
        try:
            result = storage.renamenx(args[0], args[1])
            return RESP_ONE if result else RESP_ZERO
        except Exception as e:
            return error(str(e))

    @staticmethod
    def _cmd_ttl(storage, *args):
        """TTL key - get time to live in seconds."""
        ttl = storage.ttl(args[0])
        return integer(ttl)

    @staticmethod
    def _cmd_pttl(storage, *args):
        """PTTL key - get time to live in milliseconds."""
        ttl = storage.pttl(args[0])
        return integer(ttl)

    @staticmethod
    def _cmd_expire(storage, *args):
        """EXPIRE key seconds - set expiry in seconds."""
        try:
            seconds = int(args[1])
            result = storage.expire(args[0], seconds)
            return RESP_ONE if result else RESP_ZERO
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_expireat(storage, *args):
        """EXPIREAT key timestamp - set expiry at Unix timestamp."""
        try:
            timestamp = int(args[1])
            result = storage.expireat(args[0], timestamp)
            return RESP_ONE if result else RESP_ZERO
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_pexpire(storage, *args):
        """PEXPIRE key milliseconds - set expiry in milliseconds."""
        try:
            milliseconds = int(args[1])
            result = storage.pexpire(args[0], milliseconds)
            return RESP_ONE if result else RESP_ZERO
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_pexpireat(storage, *args):
        """PEXPIREAT key ms-timestamp - set expiry at Unix ms timestamp."""
        try:
            ms_timestamp = int(args[1])
            result = storage.pexpireat(args[0], ms_timestamp)
            return RESP_ONE if result else RESP_ZERO
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_persist(storage, *args):
        """PERSIST key - remove expiry."""
        result = storage.persist(args[0])
        return RESP_ONE if result else RESP_ZERO

    # Hash command handlers

    @staticmethod
    def _cmd_hset(storage, *args):
        """HSET key field value [field value...] - set hash fields."""
        if len(args) < 3 or (len(args) - 1) % 2 != 0:
            return error("wrong number of arguments for 'hset' command")

        key = args[0]
        count = 0
        # HSET can set multiple field-value pairs
        for i in range(1, len(args), 2):
            count += HashType.hset(storage, key, args[i], args[i + 1])
        return integer(count)

    @staticmethod
    def _cmd_hget(storage, *args):
        """HGET key field - get hash field."""
        value = HashType.hget(storage, args[0], args[1])
        return bulk_string_or_null(value)

    @staticmethod
    def _cmd_hdel(storage, *args):
        """HDEL key field [field...] - delete hash fields."""
        count = HashType.hdel(storage, args[0], *args[1:])
        return integer(count)

    @staticmethod
    def _cmd_hexists(storage, *args):
        """HEXISTS key field - check field existence."""
        exists = HashType.hexists(storage, args[0], args[1])
        return RESP_ONE if exists else RESP_ZERO

    @staticmethod
    def _cmd_hgetall(storage, *args):
        """HGETALL key - get all fields and values."""
        items = HashType.hgetall(storage, args[0])
        return array([bulk_string(v) for v in items])

    @staticmethod
    def _cmd_hkeys(storage, *args):
        """HKEYS key - get all field names."""
        keys = HashType.hkeys(storage, args[0])
        return array([bulk_string(k) for k in keys])

    @staticmethod
    def _cmd_hvals(storage, *args):
        """HVALS key - get all values."""
        vals = HashType.hvals(storage, args[0])
        return array([bulk_string(v) for v in vals])

    @staticmethod
    def _cmd_hlen(storage, *args):
        """HLEN key - get number of fields."""
        length = HashType.hlen(storage, args[0])
        return integer(length)

    @staticmethod
    def _cmd_hmget(storage, *args):
        """HMGET key field [field...] - get multiple fields."""
        values = HashType.hmget(storage, args[0], *args[1:])
        return array([bulk_string_or_null(v) for v in values])

    @staticmethod
    def _cmd_hincrby(storage, *args):
        """HINCRBY key field increment - increment field by integer."""
        try:
            increment = int(args[2])
            result = HashType.hincrby(storage, args[0], args[1], increment)
            return integer(result)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_hincrbyfloat(storage, *args):
        """HINCRBYFLOAT key field increment - increment field by float."""
        try:
            increment = float(args[2])
            result = HashType.hincrbyfloat(storage, args[0], args[1], increment)
            return bulk_string(result)
        except ValueError as e:
            return error(str(e))

    # List command handlers

    @staticmethod
    def _cmd_lpush(storage, *args):
        """LPUSH key element [element...] - push to list head."""
        length = ListOperations.lpush(storage, args[0], *args[1:])
        return integer(length)

    @staticmethod
    def _cmd_rpush(storage, *args):
        """RPUSH key element [element...] - push to list tail."""
        length = ListOperations.rpush(storage, args[0], *args[1:])
        return integer(length)

    @staticmethod
    def _cmd_lpop(storage, *args):
        """LPOP key [count] - pop from list head."""
        try:
            has_count_arg = len(args) > 1
            count = int(args[1]) if has_count_arg else 1
            if not has_count_arg:
                # No count arg: return single bulk string or null
                value = ListOperations.lpop(storage, args[0])
                return bulk_string_or_null(value)
            else:
                # Count arg provided: always return array (even count=1)
                values = ListOperations.lpop(storage, args[0], count)
                if values is None:
                    return RESP_NULL
                # count=1 returns single value, wrap in list
                if not isinstance(values, list):
                    values = [values]
                return array([bulk_string(v) for v in values])
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_rpop(storage, *args):
        """RPOP key [count] - pop from list tail."""
        try:
            has_count_arg = len(args) > 1
            count = int(args[1]) if has_count_arg else 1
            if not has_count_arg:
                # No count arg: return single bulk string or null
                value = ListOperations.rpop(storage, args[0])
                return bulk_string_or_null(value)
            else:
                # Count arg provided: always return array (even count=1)
                values = ListOperations.rpop(storage, args[0], count)
                if values is None:
                    return RESP_NULL
                # count=1 returns single value, wrap in list
                if not isinstance(values, list):
                    values = [values]
                return array([bulk_string(v) for v in values])
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_llen(storage, *args):
        """LLEN key - get list length."""
        length = ListOperations.llen(storage, args[0])
        return integer(length)

    @staticmethod
    def _cmd_lrange(storage, *args):
        """LRANGE key start stop - get list range."""
        try:
            start = int(args[1])
            stop = int(args[2])
            values = ListOperations.lrange(storage, args[0], start, stop)
            return array([bulk_string(v) for v in values])
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_lindex(storage, *args):
        """LINDEX key index - get element by index."""
        try:
            index = int(args[1])
            value = ListOperations.lindex(storage, args[0], index)
            return bulk_string_or_null(value)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_lset(storage, *args):
        """LSET key index value - set element by index."""
        try:
            index = int(args[1])
            ListOperations.lset(storage, args[0], index, args[2])
            return RESP_OK
        except (ValueError, IndexError) as e:
            return error(str(e))

    @staticmethod
    def _cmd_ltrim(storage, *args):
        """LTRIM key start stop - trim list to range."""
        try:
            start = int(args[1])
            stop = int(args[2])
            ListOperations.ltrim(storage, args[0], start, stop)
            return RESP_OK
        except ValueError as e:
            return error(str(e))

    # Set command handlers

    @staticmethod
    def _cmd_sadd(storage, *args):
        """SADD key member [member...] - add to set."""
        count = SetOperations.sadd(storage, args[0], *args[1:])
        return integer(count)

    @staticmethod
    def _cmd_srem(storage, *args):
        """SREM key member [member...] - remove from set."""
        count = SetOperations.srem(storage, args[0], *args[1:])
        return integer(count)

    @staticmethod
    def _cmd_sismember(storage, *args):
        """SISMEMBER key member - check membership."""
        is_member = SetOperations.sismember(storage, args[0], args[1])
        return RESP_ONE if is_member else RESP_ZERO

    @staticmethod
    def _cmd_smembers(storage, *args):
        """SMEMBERS key - get all members."""
        members = SetOperations.smembers(storage, args[0])
        return array([bulk_string(m) for m in members])

    @staticmethod
    def _cmd_scard(storage, *args):
        """SCARD key - get set cardinality."""
        card = SetOperations.scard(storage, args[0])
        return integer(card)

    @staticmethod
    def _cmd_sinter(storage, *args):
        """SINTER key [key...] - set intersection."""
        members = SetOperations.sinter(storage, *args)
        return array([bulk_string(m) for m in members])

    @staticmethod
    def _cmd_sunion(storage, *args):
        """SUNION key [key...] - set union."""
        members = SetOperations.sunion(storage, *args)
        return array([bulk_string(m) for m in members])

    @staticmethod
    def _cmd_sdiff(storage, *args):
        """SDIFF key [key...] - set difference."""
        members = SetOperations.sdiff(storage, *args)
        return array([bulk_string(m) for m in members])

    @staticmethod
    def _cmd_sinterstore(storage, *args):
        """SINTERSTORE destination key [key...] - store intersection."""
        count = SetOperations.sinterstore(storage, args[0], *args[1:])
        return integer(count)

    @staticmethod
    def _cmd_sunionstore(storage, *args):
        """SUNIONSTORE destination key [key...] - store union."""
        count = SetOperations.sunionstore(storage, args[0], *args[1:])
        return integer(count)

    @staticmethod
    def _cmd_sdiffstore(storage, *args):
        """SDIFFSTORE destination key [key...] - store difference."""
        count = SetOperations.sdiffstore(storage, args[0], *args[1:])
        return integer(count)

    # Sorted Set command handlers

    @staticmethod
    def _cmd_zadd(storage, *args):
        """ZADD key score member [score member...] - add to sorted set."""
        if len(args) < 3 or (len(args) - 1) % 2 != 0:
            return error("wrong number of arguments for 'zadd' command")

        key = args[0]
        members = {}
        try:
            for i in range(1, len(args), 2):
                score = float(args[i])
                member = args[i + 1]
                members[member] = score

            count = SortedSetOperations.zadd(storage, key, members)
            return integer(count)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_zrem(storage, *args):
        """ZREM key member [member...] - remove from sorted set."""
        count = SortedSetOperations.zrem(storage, args[0], *args[1:])
        return integer(count)

    @staticmethod
    def _cmd_zscore(storage, *args):
        """ZSCORE key member - get member score."""
        score = SortedSetOperations.zscore(storage, args[0], args[1])
        if score is None:
            return RESP_NULL
        return bulk_string(str(score).encode())

    @staticmethod
    def _cmd_zrank(storage, *args):
        """ZRANK key member - get member rank (ascending)."""
        rank = SortedSetOperations.zrank(storage, args[0], args[1])
        if rank is None:
            return RESP_NULL
        return integer(rank)

    @staticmethod
    def _cmd_zrevrank(storage, *args):
        """ZREVRANK key member - get member rank (descending)."""
        rank = SortedSetOperations.zrevrank(storage, args[0], args[1])
        if rank is None:
            return RESP_NULL
        return integer(rank)

    @staticmethod
    def _cmd_zcard(storage, *args):
        """ZCARD key - get sorted set cardinality."""
        card = SortedSetOperations.zcard(storage, args[0])
        return integer(card)

    @staticmethod
    def _cmd_zrange(storage, *args):
        """ZRANGE key start stop [WITHSCORES] - get range by rank."""
        try:
            start = int(args[1])
            stop = int(args[2])
            withscores = len(args) > 3 and args[3].upper() == b'WITHSCORES'

            members = SortedSetOperations.zrange(storage, args[0], start, stop, withscores)

            if withscores:
                result = []
                for member, score in members:
                    result.append(bulk_string(member))
                    result.append(bulk_string(score))
                return array(result)
            else:
                return array([bulk_string(m) for m in members])
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_zrevrange(storage, *args):
        """ZREVRANGE key start stop [WITHSCORES] - get range by rank (descending)."""
        try:
            start = int(args[1])
            stop = int(args[2])
            withscores = len(args) > 3 and args[3].upper() == b'WITHSCORES'

            members = SortedSetOperations.zrevrange(storage, args[0], start, stop, withscores)

            if withscores:
                result = []
                for member, score in members:
                    result.append(bulk_string(member))
                    result.append(bulk_string(score))
                return array(result)
            else:
                return array([bulk_string(m) for m in members])
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_zincrby(storage, *args):
        """ZINCRBY key increment member - increment member score."""
        try:
            increment = float(args[1])
            new_score = SortedSetOperations.zincrby(storage, args[0], args[2], increment)
            return bulk_string(str(new_score).encode())
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_zcount(storage, *args):
        """ZCOUNT key min max - count members in score range."""
        try:
            min_score = float(args[1]) if args[1] != b'-inf' else float('-inf')
            max_score = float(args[2]) if args[2] != b'+inf' else float('inf')
            count = SortedSetOperations.zcount(storage, args[0], min_score, max_score)
            return integer(count)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_zrangebyscore(storage, *args):
        """ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]."""
        try:
            key = args[0]
            min_score = args[1]
            max_score = args[2]
            withscores = False
            offset = 0
            count = None

            i = 3
            while i < len(args):
                opt = args[i].upper()
                if opt == b'WITHSCORES':
                    withscores = True
                    i += 1
                elif opt == b'LIMIT':
                    offset = int(args[i + 1])
                    count = int(args[i + 2])
                    i += 3
                else:
                    i += 1

            members = SortedSetOperations.zrangebyscore(storage, key, min_score, max_score,
                                                        withscores=withscores, offset=offset, count=count)

            if withscores:
                result = []
                for member, score in members:
                    result.append(bulk_string(member))
                    result.append(bulk_string(str(score).encode()))
                return array(result)
            else:
                return array([bulk_string(m) for m in members])
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_zrevrangebyscore(storage, *args):
        """ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]."""
        try:
            key = args[0]
            max_score = args[1]
            min_score = args[2]
            withscores = False
            offset = 0
            count = None

            i = 3
            while i < len(args):
                opt = args[i].upper()
                if opt == b'WITHSCORES':
                    withscores = True
                    i += 1
                elif opt == b'LIMIT':
                    offset = int(args[i + 1])
                    count = int(args[i + 2])
                    i += 3
                else:
                    i += 1

            members = SortedSetOperations.zrevrangebyscore(storage, key, max_score, min_score,
                                                          withscores=withscores, offset=offset, count=count)

            if withscores:
                result = []
                for member, score in members:
                    result.append(bulk_string(member))
                    result.append(bulk_string(str(score).encode()))
                return array(result)
            else:
                return array([bulk_string(m) for m in members])
        except ValueError as e:
            return error(str(e))

    # HyperLogLog command handlers

    @staticmethod
    def _cmd_pfadd(storage, *args):
        """PFADD key element [element ...] - add elements to HyperLogLog."""
        try:
            result = HyperLogLog.pfadd(storage, args[0], *args[1:])
            return integer(result)
        except Exception as e:
            return error(str(e))

    @staticmethod
    def _cmd_pfcount(storage, *args):
        """PFCOUNT key [key ...] - estimate cardinality."""
        try:
            result = HyperLogLog.pfcount(storage, *args)
            return integer(result)
        except Exception as e:
            return error(str(e))

    @staticmethod
    def _cmd_pfmerge(storage, *args):
        """PFMERGE destkey sourcekey [sourcekey ...] - merge HyperLogLogs."""
        try:
            HyperLogLog.pfmerge(storage, args[0], *args[1:])
            return RESP_OK
        except Exception as e:
            return error(str(e))

    # Bitmap command handlers

    @staticmethod
    def _cmd_bitpos(storage, *args):
        """BITPOS key bit [start [end [BYTE|BIT]]] - find first bit set to 0 or 1."""
        try:
            key = args[0]
            bit = int(args[1])
            start = int(args[2]) if len(args) > 2 else None
            end = int(args[3]) if len(args) > 3 else None
            mode = args[4].upper() if len(args) > 4 else b'BYTE'

            result = BitmapOperations.bitpos(storage, key, bit, start, end, mode)
            return integer(result)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_bitop(storage, *args):
        """BITOP operation destkey key [key ...] - perform bitwise operation."""
        try:
            operation = args[0].upper()
            destkey = args[1]
            keys = args[2:]

            result = BitmapOperations.bitop(storage, operation, destkey, *keys)
            return integer(result)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_bitfield(storage, *args):
        """BITFIELD key [GET type offset] [SET type offset value] [INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL]."""
        try:
            results = BitmapOperations.bitfield(storage, args[0], *args[1:])
            return array([integer(r) if r is not None else RESP_NULL for r in results])
        except ValueError as e:
            return error(str(e))

    # Stream command handlers

    @staticmethod
    def _cmd_xadd(storage, *args):
        """XADD key ID field value [field value ...] - append entry to stream."""
        try:
            key = args[0]
            entry_id = args[1]

            # Parse field-value pairs
            if len(args) < 4 or (len(args) - 2) % 2 != 0:
                return error("wrong number of arguments for 'xadd' command")

            fields = {}
            for i in range(2, len(args), 2):
                fields[args[i]] = args[i + 1]

            result = StreamOperations.xadd(storage, key, entry_id, fields)
            return bulk_string(result)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_xlen(storage, *args):
        """XLEN key - get number of entries in stream."""
        try:
            length = StreamOperations.xlen(storage, args[0])
            return integer(length)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_xrange(storage, *args):
        """XRANGE key start end [COUNT count] - get range of entries."""
        try:
            key = args[0]
            start = args[1]
            end = args[2]
            count = None

            if len(args) > 4 and args[3].upper() == b'COUNT':
                count = int(args[4])

            entries = StreamOperations.xrange(storage, key, start, end, count)

            # Format: [[id, [field, value, ...]], ...]
            result = []
            for entry_id, fields in entries:
                field_list = []
                for field, value in fields.items():
                    field_list.append(bulk_string(field))
                    field_list.append(bulk_string(value))
                result.append(array([bulk_string(entry_id), array(field_list)]))

            return array(result)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_xrevrange(storage, *args):
        """XREVRANGE key end start [COUNT count] - get range of entries in reverse."""
        try:
            key = args[0]
            end = args[1]
            start = args[2]
            count = None

            if len(args) > 4 and args[3].upper() == b'COUNT':
                count = int(args[4])

            entries = StreamOperations.xrevrange(storage, key, end, start, count)

            # Format: [[id, [field, value, ...]], ...]
            result = []
            for entry_id, fields in entries:
                field_list = []
                for field, value in fields.items():
                    field_list.append(bulk_string(field))
                    field_list.append(bulk_string(value))
                result.append(array([bulk_string(entry_id), array(field_list)]))

            return array(result)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_xread(storage, *args):
        """XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] ID [ID ...] - read from streams."""
        try:
            count = None
            block = None
            streams_idx = -1

            # Parse options
            i = 0
            while i < len(args):
                opt = args[i].upper()
                if opt == b'COUNT':
                    count = int(args[i + 1])
                    i += 2
                elif opt == b'BLOCK':
                    block = int(args[i + 1])
                    i += 2
                elif opt == b'STREAMS':
                    streams_idx = i + 1
                    break
                else:
                    i += 1

            if streams_idx == -1:
                return error("syntax error")

            # Parse streams and IDs
            remaining = args[streams_idx:]
            if len(remaining) % 2 != 0:
                return error("wrong number of arguments for 'xread' command")

            mid = len(remaining) // 2
            keys = remaining[:mid]
            ids = remaining[mid:]

            streams_dict = dict(zip(keys, ids))
            result = StreamOperations.xread(storage, streams_dict, count, block)

            if result is None:
                return RESP_NULL

            # Format result
            output = []
            for stream_key, entries in result:
                entries_arr = []
                for entry_id, fields in entries:
                    field_list = []
                    for field, value in fields.items():
                        field_list.append(bulk_string(field))
                        field_list.append(bulk_string(value))
                    entries_arr.append(array([bulk_string(entry_id), array(field_list)]))
                output.append(array([bulk_string(stream_key), array(entries_arr)]))

            return array(output)
        except ValueError as e:
            return error(str(e))

    @staticmethod
    def _cmd_xtrim(storage, *args):
        """XTRIM key MAXLEN [~] count - trim stream to maximum length."""
        try:
            key = args[0]

            # Parse MAXLEN option
            if len(args) < 3 or args[1].upper() != b'MAXLEN':
                return error("syntax error")

            approximate = False
            maxlen_idx = 2

            if args[2] == b'~':
                approximate = True
                maxlen_idx = 3

            if maxlen_idx >= len(args):
                return error("syntax error")

            maxlen = int(args[maxlen_idx])
            trimmed = StreamOperations.xtrim(storage, key, maxlen, approximate)
            return integer(trimmed)
        except ValueError as e:
            return error(str(e))

    # Additional Set command handlers

    @staticmethod
    def _cmd_spop(storage, *args):
        """SPOP key [count] - remove and return random members."""
        try:
            count = int(args[1]) if len(args) > 1 else 1
            result = SetOperations.spop(storage, args[0], count)

            if result is None:
                return RESP_NULL

            if count == 1 and len(args) == 1:
                # Single element without count returns as bulk string
                return bulk_string(result)
            elif isinstance(result, list):
                return array([bulk_string(m) for m in result])
            else:
                return bulk_string(result)
        except TypeError as e:
            return error(str(e))

    @staticmethod
    def _cmd_srandmember(storage, *args):
        """SRANDMEMBER key [count] - get random members without removing."""
        try:
            count = int(args[1]) if len(args) > 1 else None
            result = SetOperations.srandmember(storage, args[0], count)

            if result is None:
                return RESP_NULL

            if count is None:
                return bulk_string(result)
            elif isinstance(result, list):
                return array([bulk_string(m) for m in result])
            else:
                return bulk_string(result)
        except TypeError as e:
            return error(str(e))

    @staticmethod
    def _cmd_smove(storage, *args):
        """SMOVE source destination member - move member between sets."""
        try:
            result = SetOperations.smove(storage, args[0], args[1], args[2])
            return RESP_ONE if result else RESP_ZERO
        except TypeError as e:
            return error(str(e))

    # Server command handlers

    @staticmethod
    def _cmd_dbsize(storage, *args):
        """DBSIZE - return the number of keys in the database."""
        count = len(storage._data)
        return integer(count)

    @staticmethod
    def _cmd_flushdb(storage, *args):
        """FLUSHDB [ASYNC|SYNC] - remove all keys from the database."""
        storage.flush()
        return RESP_OK

    @staticmethod
    def _cmd_time(storage, *args):
        """TIME - return current server time."""
        import time
        current = time.time()
        seconds = int(current)
        microseconds = int((current - seconds) * 1000000)
        return array([bulk_string(str(seconds).encode()), bulk_string(str(microseconds).encode())])

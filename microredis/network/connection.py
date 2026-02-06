"""
MicroRedis Async Connection Handler

Implements async TCP connection handling for Redis protocol (RESP2) on ESP32-S3.
Manages client connections, command parsing, and response generation with
memory-efficient patterns for MicroPython.

Platform: ESP32-S3 with MicroPython
Memory optimization: __slots__, pre-allocated buffers, zero-copy parsing
"""

# Platform-specific async import - uasyncio on MicroPython, asyncio on CPython
try:
    import uasyncio as asyncio
except ImportError:
    import asyncio

# Import const() with fallback for PC-based testing
try:
    from micropython import const
except ImportError:
    const = lambda x: x

import time
import gc

from ..core.protocol import RESPParser
from ..core.response import (
    # Pre-allocated responses
    RESP_OK, RESP_PONG, RESP_NULL, RESP_EMPTY_ARRAY,
    # Error responses
    ERR_UNKNOWN_CMD, ERR_WRONG_ARITY, ERR_SYNTAX, ERR_NOT_INTEGER,
    # Builder functions
    simple_string, error, integer, bulk_string, bulk_string_or_null,
    array, encode_value, ResponseBuilder
)
from ..core.constants import BUFFER_SIZE, CRLF

# Connection timeout constants
READ_TIMEOUT_MS = const(30000)  # 30 second timeout for read operations
WRITE_TIMEOUT_MS = const(5000)  # 5 second timeout for write operations


class ClientConnection:
    """
    Represents a single client connection with parsing state.

    Manages the lifecycle of a client TCP connection including reading commands,
    writing responses, and maintaining connection state (auth, transactions, etc).

    Memory optimizations:
    - __slots__ to reduce instance overhead
    - Pre-allocated 4KB buffer for incoming data
    - Single RESPParser instance per connection
    - Zero-copy buffer operations via memoryview
    """

    __slots__ = (
        'reader',           # StreamReader: Async reader for socket
        'writer',           # StreamWriter: Async writer for socket
        'parser',           # RESPParser: Protocol parser instance
        'addr',             # tuple: Client address (ip, port)
        'authenticated',    # bool: Authentication state
        'in_transaction',   # bool: MULTI/EXEC transaction state
        'watched_keys',     # set: Keys watched for optimistic locking
        'subscriptions',    # set: Pub/sub channel subscriptions
        '_closed',          # bool: Connection closed flag
    )

    def __init__(self, reader, writer, addr):
        """
        Initialize client connection.

        Args:
            reader: asyncio.StreamReader for reading from socket
            writer: asyncio.StreamWriter for writing to socket
            addr: tuple (ip, port) of client address
        """
        self.reader = reader
        self.writer = writer
        self.addr = addr

        # Create protocol parser instance
        self.parser = RESPParser()

        # Connection state
        self.authenticated = False
        self.in_transaction = False
        self.watched_keys = set()
        self.subscriptions = set()
        self._closed = False

    async def read_command(self):
        """
        Read and parse next command from client.

        Returns:
            tuple: (command: str, args: list[bytes]) if successful
            None: If connection closed or timeout

        Raises:
            asyncio.TimeoutError: If read timeout exceeded
            OSError: On socket errors
        """
        if self._closed:
            return None

        # Use loop instead of recursion to prevent stack overflow
        while True:
            try:
                # Read data with timeout
                data = await asyncio.wait_for(
                    self.reader.read(BUFFER_SIZE),
                    timeout=READ_TIMEOUT_MS / 1000.0
                )

                # Connection closed by client
                if not data:
                    return None

                # Feed data to parser
                self.parser.feed(data)

                # Try to parse complete command
                result = self.parser.parse()

                # If we got a result, return it
                if result:
                    command, args = result
                    return (command, args)

                # Need more data - continue loop

            except asyncio.TimeoutError:
                # Timeout - return None to trigger disconnect
                return None

            except OSError:
                # Socket error - connection lost
                return None

    async def write_response(self, data):
        """
        Write response data to client.

        Args:
            data: bytes - RESP2-encoded response to send

        Raises:
            OSError: On socket errors
        """
        if self._closed:
            return

        try:
            # Write data with timeout
            self.writer.write(data)
            await asyncio.wait_for(
                self.writer.drain(),
                timeout=WRITE_TIMEOUT_MS / 1000.0
            )

        except asyncio.TimeoutError:
            # Write timeout - close connection
            await self.close()

        except OSError:
            # Socket error - mark as closed
            self._closed = True

    async def close(self):
        """
        Close the client connection gracefully.

        Cleans up resources and closes the socket.
        """
        if self._closed:
            return

        self._closed = True

        try:
            # Close the writer (also closes the socket)
            self.writer.close()
            if hasattr(self.writer, 'wait_closed'):
                await self.writer.wait_closed()
        except (OSError, AttributeError, RuntimeError):
            # Ignore common errors during close:
            # - OSError: socket already closed
            # - AttributeError: writer not initialized
            # - RuntimeError: event loop issues
            pass

        # Clear parser state to free memory
        self.parser.reset()

        # Clear sets to free memory
        self.watched_keys.clear()
        self.subscriptions.clear()

        # Trigger garbage collection to reclaim memory
        gc.collect()

    def is_connected(self):
        """
        Check if connection is still open.

        Returns:
            bool: True if connection is open, False otherwise
        """
        return not self._closed


class ConnectionHandler:
    """
    Handles client connections and command execution.

    Manages the command dispatch table and coordinates between the network layer
    and the storage engine. Uses CommandRouter for full command support.

    Memory optimizations:
    - __slots__ to reduce instance overhead
    - Reusable ResponseBuilder for complex responses
    - Delegates command execution to CommandRouter
    """

    __slots__ = (
        'storage',          # Storage: Storage engine instance
        'router',           # CommandRouter: Command dispatch router
        'pubsub',           # PubSubManager: Pub/Sub manager
        'transactions',     # TransactionManager: Transaction manager
        'response_builder', # ResponseBuilder: Reusable response builder
        'start_time',       # float: Server start timestamp for INFO
        'middleware',       # MiddlewareChain: Request processing middleware
        'config',           # Config: Server configuration
    )

    def __init__(self, storage, router=None, pubsub_manager=None,
                 transaction_manager=None, config=None):
        """
        Initialize connection handler.

        Args:
            storage: Storage engine instance for data operations
            router: CommandRouter instance for command dispatch
            pubsub_manager: PubSubManager instance for pub/sub
            transaction_manager: TransactionManager instance for transactions
            config: Optional Config instance for server settings
        """
        self.storage = storage
        self.router = router
        self.pubsub = pubsub_manager
        self.transactions = transaction_manager
        self.response_builder = ResponseBuilder()
        self.start_time = time.time()
        self.config = config

        # Initialize middleware chain
        self._setup_middleware(config)

    def _setup_middleware(self, config):
        """
        Set up middleware chain based on configuration.

        Args:
            config: Config instance or None
        """
        from .middleware import MiddlewareChain, AuthMiddleware, RequestValidator, RateLimiter

        self.middleware = MiddlewareChain()

        # Add authentication middleware if password is configured
        password = config.get('requirepass') if config else None
        if password:
            self.middleware.add(AuthMiddleware(password))

        # Add request validator
        self.middleware.add(RequestValidator(
            max_bulk_size=512 * 1024,  # 512KB max bulk size for ESP32
            max_args=100
        ))

        # Rate limiter is optional - can be enabled via config
        # For ESP32 with limited clients, usually not needed
        # if config and config.get('rate_limit_enabled'):
        #     self.middleware.add(RateLimiter(max_requests=1000, window_ms=1000))

    # Commands allowed in pub/sub mode
    PUBSUB_ALLOWED_CMDS = {
        b'SUBSCRIBE', b'UNSUBSCRIBE', b'PSUBSCRIBE', b'PUNSUBSCRIBE',
        b'PING', b'QUIT'
    }

    # Commands that cannot be executed in MULTI block
    TRANSACTION_FORBIDDEN_CMDS = {b'WATCH', b'MULTI'}

    async def handle_client(self, reader, writer):
        """
        Handle a client connection from start to finish.

        Args:
            reader: asyncio.StreamReader for socket input
            writer: asyncio.StreamWriter for socket output
        """
        # Get client address
        addr = writer.get_extra_info('peername', ('unknown', 0))

        # Create connection object
        conn = ClientConnection(reader, writer, addr)

        try:
            # Main command loop
            while conn.is_connected():
                # Read next command
                result = await conn.read_command()

                # Connection closed or timeout
                if result is None:
                    break

                command, args = result

                # Skip empty commands
                if not command:
                    continue

                # Execute command and get response
                response = await self.execute_command(conn, command, args)

                # Write response to client
                await conn.write_response(response)

                # Check if QUIT command was issued
                if command == 'QUIT':
                    break

        except Exception as e:
            # Log error (in production, send to logging system)
            print(f"Error handling client {addr}: {e}")

        finally:
            # Clean up client state on disconnect
            if self.transactions:
                self.transactions.cleanup_client(conn)
            if self.pubsub:
                self.pubsub.unsubscribe_all(conn)

            # Always close connection on exit
            await conn.close()

    async def execute_command(self, conn, cmd, args):
        """
        Execute a command and return RESP2-encoded response.

        Args:
            conn: ClientConnection instance
            cmd: str - Command name (uppercase)
            args: list[bytes] - Command arguments

        Returns:
            bytes: RESP2-encoded response
        """
        # Convert command to bytes for middleware and router
        cmd_bytes = cmd.encode('utf-8') if isinstance(cmd, str) else cmd
        cmd_upper = cmd.upper() if isinstance(cmd, str) else cmd.decode('utf-8').upper()

        # Process through middleware chain
        middleware_error = self.middleware.process(conn, cmd_bytes, args)
        if middleware_error:
            return middleware_error

        # Handle AUTH command specially (middleware handles validation)
        if cmd_upper == 'AUTH':
            from .middleware import AuthMiddleware
            for mw in self.middleware._middlewares:
                if isinstance(mw, AuthMiddleware):
                    return mw.handle_auth(conn, args)
            # No auth middleware - auth not configured
            return error('ERR Client sent AUTH, but no password is set')

        # Check if client is in pub/sub mode
        if self.pubsub and self.pubsub.is_subscribed(conn):
            if cmd_bytes.upper() not in self.PUBSUB_ALLOWED_CMDS:
                return error('ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context')

        # Handle transaction commands specially
        if self.transactions:
            # WATCH command
            if cmd_upper == 'WATCH':
                return self.transactions.watch(conn, *args)

            # UNWATCH command
            if cmd_upper == 'UNWATCH':
                return self.transactions.unwatch(conn)

            # MULTI command
            if cmd_upper == 'MULTI':
                return self.transactions.multi(conn)

            # EXEC command
            if cmd_upper == 'EXEC':
                return self.transactions.exec(conn, self.router)

            # DISCARD command
            if cmd_upper == 'DISCARD':
                return self.transactions.discard(conn)

            # If in transaction mode, queue the command (with arity validation)
            if self.transactions.is_in_transaction(conn):
                queued = self.transactions.queue_command(conn, cmd_bytes.upper(), args, router=self.router)
                if queued is not None:
                    return queued

        # Handle pub/sub commands
        if self.pubsub:
            if cmd_upper == 'SUBSCRIBE':
                responses = self.pubsub.subscribe(conn, *args)
                return b''.join(responses)

            if cmd_upper == 'UNSUBSCRIBE':
                responses = self.pubsub.unsubscribe(conn, *args)
                return b''.join(responses)

            if cmd_upper == 'PSUBSCRIBE':
                responses = self.pubsub.psubscribe(conn, *args)
                return b''.join(responses)

            if cmd_upper == 'PUNSUBSCRIBE':
                responses = self.pubsub.punsubscribe(conn, *args)
                return b''.join(responses)

            if cmd_upper == 'PUBLISH':
                if len(args) < 2:
                    return ERR_WRONG_ARITY
                count = await self.pubsub.publish(args[0], args[1])
                return integer(count)

        # Use CommandRouter for all other commands
        if self.router:
            return self.router.execute(cmd_bytes, args)

        # Fallback for basic commands if no router
        return self._execute_basic_command(conn, cmd_upper, args)

    def _execute_basic_command(self, conn, cmd, args):
        """
        Execute basic built-in commands (fallback when no router).

        Args:
            conn: ClientConnection instance
            cmd: str - Command name (uppercase)
            args: list[bytes] - Command arguments

        Returns:
            bytes: RESP2-encoded response
        """
        if cmd == 'PING':
            if args:
                return bulk_string(args[0])
            return RESP_PONG

        if cmd == 'ECHO':
            if not args:
                return ERR_WRONG_ARITY
            return bulk_string(args[0])

        if cmd == 'QUIT':
            return RESP_OK

        if cmd == 'GET':
            if len(args) != 1:
                return ERR_WRONG_ARITY
            try:
                value = self.storage.get(args[0])
            except TypeError as e:
                return error(str(e))
            return bulk_string_or_null(value)

        if cmd == 'SET':
            if len(args) < 2:
                return ERR_WRONG_ARITY
            key, value = args[0], args[1]
            ex, px, nx, xx = None, None, False, False
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
                    return ERR_SYNTAX
            if self.storage.set(key, value, ex=ex, px=px, nx=nx, xx=xx):
                return RESP_OK
            return RESP_NULL

        if cmd == 'DEL':
            if not args:
                return ERR_WRONG_ARITY
            count = self.storage.delete(*args)
            return integer(count)

        if cmd == 'EXISTS':
            if not args:
                return ERR_WRONG_ARITY
            count = self.storage.exists(*args)
            return integer(count)

        return ERR_UNKNOWN_CMD

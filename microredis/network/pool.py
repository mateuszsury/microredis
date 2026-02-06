"""
MicroRedis Connection Pool Module

Pre-allocated connection pool for efficient memory management on ESP32-S3.
Allocates all buffers and parsers at startup to avoid fragmentation during
runtime operations.

Platform: ESP32-S3 with MicroPython
Memory optimization: Pre-allocation, __slots__, zero-copy resource reuse
"""

from microredis.core.protocol import RESPParser
from microredis.core.constants import MAX_CLIENTS, BUFFER_SIZE


class ConnectionPool:
    """
    Pre-allocated connection pool for MicroRedis server.

    Allocates all connection resources (buffers, parsers) at initialization
    to prevent memory fragmentation during runtime. Designed for memory-
    constrained ESP32-S3 environment.

    Memory strategy:
    - All buffers pre-allocated as bytearrays
    - All RESP parsers pre-allocated
    - Simple slot-based allocation/deallocation
    - No dynamic memory allocation during acquire/release

    Usage:
        pool = ConnectionPool(max_connections=8)

        # Acquire connection slot
        result = pool.acquire()
        if result:
            slot_id, buffer, parser = result
            conn = ClientConnection(reader, writer, buffer, parser, addr)
            pool.set_connection(slot_id, conn)

        # Release when done
        pool.release(slot_id)

    Or with context manager:
        with pool.acquire_context() as ctx:
            # ctx.slot_id, ctx.buffer, ctx.parser available
            pass
    """

    __slots__ = (
        '_buffers',          # list[bytearray]: Pre-allocated 4KB buffers
        '_parsers',          # list[RESPParser]: Pre-allocated parsers
        '_available',        # set[int]: Available slot indices
        '_in_use',           # dict[int, ClientConnection]: Active connections
        '_max_connections',  # int: Maximum concurrent connections
        '_buffer_size',      # int: Size of each buffer in bytes
    )

    def __init__(self, max_connections=MAX_CLIENTS, buffer_size=BUFFER_SIZE):
        """
        Initialize connection pool with pre-allocated resources.

        Args:
            max_connections: Maximum number of concurrent connections (default: 8)
            buffer_size: Size of each buffer in bytes (default: 4096)

        Memory allocation:
            total_memory = max_connections * buffer_size
            Example: 8 connections * 4KB = 32KB of buffer memory

        All resources are allocated immediately to prevent fragmentation.
        """
        self._max_connections = max_connections
        self._buffer_size = buffer_size

        # Pre-allocate all buffers at startup
        # This prevents memory fragmentation during runtime operations
        self._buffers = [bytearray(buffer_size) for _ in range(max_connections)]

        # Pre-allocate all RESP parsers
        # Each parser has its own internal buffer and state
        self._parsers = [RESPParser() for _ in range(max_connections)]

        # Initialize all slots as available
        self._available = set(range(max_connections))

        # Track active connections by slot ID
        self._in_use = {}

    def acquire(self):
        """
        Acquire a connection slot with pre-allocated resources.

        Returns:
            tuple: (slot_id, buffer, parser) if slot available
                   slot_id: int - Unique slot identifier (0 to max_connections-1)
                   buffer: bytearray - Pre-allocated buffer for this slot
                   parser: RESPParser - Pre-allocated parser for this slot
            None: If pool is full (all slots in use)

        Note:
            The buffer is NOT zeroed on acquire for performance.
            The parser IS reset to clean state.
            Caller is responsible for setting up ClientConnection.
        """
        if not self._available:
            return None

        # Pop any available slot (set.pop() is O(1) average)
        slot_id = self._available.pop()

        # Reset parser to clean state for reuse
        # Parser maintains internal state that must be cleared
        self._parsers[slot_id].reset()

        # Return slot resources
        # Buffer is reused as-is (will be overwritten by new data)
        return slot_id, self._buffers[slot_id], self._parsers[slot_id]

    def release(self, slot_id):
        """
        Release a connection slot back to the pool.

        Args:
            slot_id: int - Slot identifier to release

        Note:
            Removes connection from tracking and marks slot as available.
            Does NOT clear buffer or parser (done on next acquire).
            Safe to call multiple times with same slot_id.
        """
        # Remove from active connections if present
        if slot_id in self._in_use:
            del self._in_use[slot_id]

        # Mark slot as available for reuse
        # Idempotent - safe to add multiple times
        self._available.add(slot_id)

    def get_connection(self, slot_id):
        """
        Retrieve active connection by slot ID.

        Args:
            slot_id: int - Slot identifier

        Returns:
            ClientConnection: Active connection object
            None: If slot is not in use
        """
        return self._in_use.get(slot_id)

    def set_connection(self, slot_id, conn):
        """
        Associate a ClientConnection with a slot.

        Args:
            slot_id: int - Slot identifier
            conn: ClientConnection - Connection object to track

        Note:
            Should be called after acquire() to register the connection.
            Allows pool to track active connections for statistics.
        """
        self._in_use[slot_id] = conn

    def get_stats(self):
        """
        Get connection pool statistics.

        Returns:
            dict: Pool statistics with keys:
                - max_connections: Total pool capacity
                - active_connections: Number of connections in use
                - available_slots: Number of free slots
                - buffer_size: Size of each buffer in bytes
                - total_buffer_memory: Total memory allocated for buffers

        Useful for monitoring and INFO command implementation.
        """
        return {
            'max_connections': self._max_connections,
            'active_connections': len(self._in_use),
            'available_slots': len(self._available),
            'buffer_size': self._buffer_size,
            'total_buffer_memory': self._max_connections * self._buffer_size,
        }

    def is_full(self):
        """
        Check if connection pool is at capacity.

        Returns:
            bool: True if all slots are in use, False if slots available
        """
        return len(self._available) == 0

    def available_slots(self):
        """
        Get number of available connection slots.

        Returns:
            int: Number of free slots that can be acquired
        """
        return len(self._available)


class PooledConnection:
    """
    Context manager for acquiring pooled connection resources.

    Automatically acquires a slot on entry and releases on exit.
    Raises RuntimeError if pool is exhausted.

    Usage:
        pool = ConnectionPool()

        with PooledConnection(pool) as ctx:
            # ctx.slot_id, ctx.buffer, ctx.parser available
            reader, writer = await asyncio.open_connection(...)
            conn = ClientConnection(reader, writer, ctx.buffer, ctx.parser, addr)
            pool.set_connection(ctx.slot_id, conn)
            # Use connection...
        # Automatically released on exit
    """

    __slots__ = (
        '_pool',      # ConnectionPool: Pool instance
        '_slot_id',   # int: Acquired slot ID (None if not acquired)
        'buffer',     # bytearray: Acquired buffer
        'parser',     # RESPParser: Acquired parser
    )

    def __init__(self, pool):
        """
        Initialize context manager.

        Args:
            pool: ConnectionPool - Pool to acquire from
        """
        self._pool = pool
        self._slot_id = None
        self.buffer = None
        self.parser = None

    @property
    def slot_id(self):
        """Get the acquired slot ID."""
        return self._slot_id

    def __enter__(self):
        """
        Acquire pool slot on context entry.

        Returns:
            PooledConnection: Self with buffer and parser set

        Raises:
            RuntimeError: If connection pool is exhausted
        """
        result = self._pool.acquire()
        if result is None:
            raise RuntimeError("Connection pool exhausted")

        self._slot_id, self.buffer, self.parser = result
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Release pool slot on context exit.

        Args:
            exc_type: Exception type (if exception occurred)
            exc_val: Exception value
            exc_tb: Exception traceback

        Returns:
            False: Allow exception propagation

        Note:
            Slot is released even if exception occurred.
        """
        if self._slot_id is not None:
            self._pool.release(self._slot_id)
            self._slot_id = None
            self.buffer = None
            self.parser = None

        return False  # Don't suppress exceptions

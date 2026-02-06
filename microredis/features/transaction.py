"""
MicroRedis Transaction Module

Implements WATCH/MULTI/EXEC/DISCARD transaction support with optimistic locking
for MicroRedis on ESP32-S3 with MicroPython.

Key Features:
- WATCH: Optimistic locking via version tracking
- MULTI: Begin transaction (command queueing)
- EXEC: Execute transaction atomically (abort on WATCH conflicts)
- DISCARD: Abort transaction
- Memory-efficient state management per connection

Platform: ESP32-S3 with MicroPython
Memory optimizations: __slots__, minimal per-client overhead

Transaction Semantics:
1. WATCH key [key ...] - Mark keys for optimistic locking
2. MULTI - Enter transaction mode (commands queued)
3. <commands> - Commands queued, return QUEUED
4. EXEC - Execute all queued commands atomically
   - Aborts (returns null array) if any WATCHed key was modified
   - Returns array of command results on success
5. DISCARD - Abort transaction, clear queue and watches
"""

from microredis.core.response import (
    RESP_OK,
    RESP_QUEUED,
    RESP_NULL_ARRAY,
    error,
    array,
)


class TransactionState:
    """
    Per-connection transaction state.

    Tracks transaction mode, queued commands, watched keys with versions,
    and error state for a single client connection.

    Memory: Minimal overhead with __slots__, state only exists when needed
    """

    __slots__ = (
        'in_multi',         # bool: True if inside MULTI block
        'command_queue',    # list[tuple[bytes, list]]: Queued (cmd, args) pairs
        'watched_keys',     # dict[bytes, int]: key -> version at WATCH time
        'error_state',      # bool: True if error occurred during MULTI
    )

    def __init__(self):
        """Initialize empty transaction state."""
        self.in_multi = False
        self.command_queue = []
        self.watched_keys = {}
        self.error_state = False

    def reset(self):
        """Reset state to initial values (for reuse after EXEC/DISCARD)."""
        self.in_multi = False
        self.command_queue.clear()
        self.watched_keys.clear()
        self.error_state = False


class TransactionManager:
    """
    Manages transactions for all client connections.

    Handles WATCH/MULTI/EXEC/DISCARD commands with optimistic locking
    via Storage._version tracking. Each client connection can have
    independent transaction state.

    Usage:
        txn_mgr = TransactionManager(storage)

        # Client calls WATCH
        response = txn_mgr.watch(connection, b'key1', b'key2')

        # Client calls MULTI
        response = txn_mgr.multi(connection)

        # Commands are queued
        queued = txn_mgr.queue_command(connection, b'SET', [b'key1', b'value'])
        if queued:
            return queued  # Returns RESP_QUEUED

        # Execute transaction
        response = txn_mgr.exec(connection, command_router)

    Memory: O(connections) state overhead, lazy allocation
    """

    __slots__ = ('_storage', '_client_state')

    def __init__(self, storage):
        """
        Initialize transaction manager.

        Args:
            storage: Storage instance for version tracking
        """
        self._storage = storage
        # Map connection objects to their transaction state
        # Only allocate state when client uses WATCH/MULTI
        self._client_state = {}

    # =========================================================================
    # State Management Helpers
    # =========================================================================

    def _get_or_create_state(self, connection):
        """
        Get or create transaction state for a connection.

        Args:
            connection: Client connection object (used as dict key)

        Returns:
            TransactionState: State for this connection
        """
        if connection not in self._client_state:
            self._client_state[connection] = TransactionState()
        return self._client_state[connection]

    def _reset_state(self, connection):
        """
        Reset transaction state for a connection.

        Args:
            connection: Client connection object
        """
        if connection in self._client_state:
            self._client_state[connection].reset()

    def cleanup_client(self, connection):
        """
        Clean up transaction state when client disconnects.

        Should be called by connection handler on disconnect.

        Args:
            connection: Client connection object
        """
        if connection in self._client_state:
            del self._client_state[connection]

    def is_in_transaction(self, connection):
        """
        Check if connection is in MULTI transaction mode.

        Args:
            connection: Client connection object

        Returns:
            bool: True if in MULTI mode, False otherwise
        """
        state = self._client_state.get(connection)
        return state is not None and state.in_multi

    # =========================================================================
    # WATCH Command - Optimistic Locking
    # =========================================================================

    def watch(self, connection, *keys):
        """
        Mark keys for optimistic locking (WATCH command).

        Records the current version of each key. If any key is modified
        before EXEC, the transaction will abort.

        Redis semantics:
        - WATCH can be called multiple times, adds to watched set
        - WATCH inside MULTI is not allowed
        - WATCH is cleared by EXEC, DISCARD, or UNWATCH
        - Watched keys are checked at EXEC time

        Args:
            connection: Client connection object
            *keys: bytes - Keys to watch

        Returns:
            bytes: RESP_OK or error response
        """
        state = self._get_or_create_state(connection)

        # Cannot WATCH inside MULTI
        if state.in_multi:
            return error("ERR WATCH inside MULTI is not allowed")

        # Record current version for each key
        for key in keys:
            # Get current version from storage (0 if key doesn't exist)
            version = self._storage._version.get(key, 0)
            state.watched_keys[key] = version

        return RESP_OK

    def unwatch(self, connection):
        """
        Clear all watched keys (UNWATCH command).

        Args:
            connection: Client connection object

        Returns:
            bytes: RESP_OK
        """
        state = self._client_state.get(connection)
        if state:
            state.watched_keys.clear()

        return RESP_OK

    # =========================================================================
    # MULTI Command - Begin Transaction
    # =========================================================================

    def multi(self, connection):
        """
        Enter transaction mode (MULTI command).

        After MULTI, all commands are queued instead of executed
        until EXEC or DISCARD is called.

        Redis semantics:
        - Nested MULTI not allowed
        - Commands are queued and return QUEUED
        - Some commands (WATCH, MULTI) are not allowed inside MULTI
        - EXEC executes all queued commands atomically

        Args:
            connection: Client connection object

        Returns:
            bytes: RESP_OK or error if already in MULTI
        """
        state = self._get_or_create_state(connection)

        # Check for nested MULTI
        if state.in_multi:
            return error("ERR MULTI calls can not be nested")

        # Enter transaction mode
        state.in_multi = True
        state.command_queue = []
        state.error_state = False

        return RESP_OK

    # =========================================================================
    # Command Queueing (called from ConnectionHandler)
    # =========================================================================

    def queue_command(self, connection, cmd, args, router=None):
        """
        Queue a command if in transaction mode.

        Called by connection handler for every command. If in MULTI mode,
        validates arity and queues the command instead of executing it.

        Args:
            connection: Client connection object
            cmd: bytes - Command name (uppercase)
            args: list - Command arguments
            router: CommandRouter | None - Router for arity validation

        Returns:
            bytes | None: RESP_QUEUED if queued, error if invalid, None if not in transaction
        """
        state = self._client_state.get(connection)

        # Not in transaction mode - execute normally
        if state is None or not state.in_multi:
            return None

        # Commands that are not allowed inside MULTI
        forbidden_commands = {b'WATCH', b'MULTI'}
        if cmd in forbidden_commands:
            state.error_state = True
            return error(f"ERR {cmd.decode()} inside MULTI is not allowed")

        # Validate command exists and arity via router
        if router is not None:
            cmd_info = router.get_command_info(cmd)
            if cmd_info is None:
                state.error_state = True
                cmd_name = cmd.decode() if isinstance(cmd, bytes) else cmd
                return error(f"ERR unknown command '{cmd_name}'")
            if not router._check_arity(cmd_info, args):
                state.error_state = True
                cmd_name = cmd.decode() if isinstance(cmd, bytes) else cmd
                return error(f"ERR wrong number of arguments for '{cmd_name}' command")

        # Queue the command
        state.command_queue.append((cmd, args))

        return RESP_QUEUED

    # =========================================================================
    # EXEC Command - Execute Transaction
    # =========================================================================

    def exec(self, connection, command_router):
        """
        Execute queued transaction (EXEC command).

        Checks for WATCH conflicts, then executes all queued commands
        atomically. Resets transaction state after execution.

        Redis semantics:
        - Returns null array if any WATCHed key was modified
        - Returns error if errors occurred during MULTI
        - Returns array of results on success
        - Clears WATCH and MULTI state after execution

        Args:
            connection: Client connection object
            command_router: Object with execute(cmd, args) method

        Returns:
            bytes: RESP array of results, null array on conflict, or error
        """
        state = self._client_state.get(connection)

        # EXEC without MULTI
        if state is None or not state.in_multi:
            return error("ERR EXEC without MULTI")

        # Check for WATCH conflicts (optimistic locking)
        for key, old_version in state.watched_keys.items():
            current_version = self._storage._version.get(key, 0)
            if current_version != old_version:
                # Version mismatch - key was modified
                self._reset_state(connection)
                return RESP_NULL_ARRAY

        # Check for errors during MULTI
        if state.error_state:
            self._reset_state(connection)
            return error("EXECABORT Transaction discarded because of previous errors")

        # Execute all queued commands
        results = []
        for cmd, args in state.command_queue:
            try:
                # Execute command via router (signature: cmd, args)
                result = command_router.execute(cmd, args)
                results.append(result)
            except Exception as e:
                # Command execution failed - add error to results
                # In Redis, EXEC continues even if individual commands fail
                error_msg = f"ERR {str(e)}"
                results.append(error(error_msg))

        # Reset transaction state
        self._reset_state(connection)

        # Return array of results
        return array(results)

    # =========================================================================
    # DISCARD Command - Abort Transaction
    # =========================================================================

    def discard(self, connection):
        """
        Abort transaction and clear queue (DISCARD command).

        Clears command queue and WATCH state, exits MULTI mode.

        Args:
            connection: Client connection object

        Returns:
            bytes: RESP_OK or error if not in MULTI
        """
        state = self._client_state.get(connection)

        # DISCARD without MULTI
        if state is None or not state.in_multi:
            return error("ERR DISCARD without MULTI")

        # Reset state
        self._reset_state(connection)

        return RESP_OK

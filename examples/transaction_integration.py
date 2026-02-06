"""
Transaction Integration Example

Shows how to integrate TransactionManager into ConnectionHandler.
This demonstrates the modification pattern for the connection handler.

Platform: ESP32-S3 with MicroPython
"""

# Example integration pattern for ConnectionHandler

# ============================================================================
# 1. Add TransactionManager to ConnectionHandler __init__
# ============================================================================

"""
from microredis.features.transaction import TransactionManager

class ConnectionHandler:
    __slots__ = (
        'storage',
        'response_builder',
        'command_table',
        'transaction_mgr',    # <-- ADD THIS
    )

    def __init__(self, storage):
        self.storage = storage
        self.response_builder = ResponseBuilder()
        self.transaction_mgr = TransactionManager(storage)  # <-- ADD THIS
        self.command_table = self._build_command_table()
"""

# ============================================================================
# 2. Add transaction commands to command table
# ============================================================================

"""
def _build_command_table(self):
    return {
        # ... existing commands ...

        # Transaction commands
        b'WATCH': (self._cmd_watch, 1, -1),      # WATCH key [key ...]
        b'UNWATCH': (self._cmd_unwatch, 0, 0),   # UNWATCH
        b'MULTI': (self._cmd_multi, 0, 0),       # MULTI
        b'EXEC': (self._cmd_exec, 0, 0),         # EXEC
        b'DISCARD': (self._cmd_discard, 0, 0),   # DISCARD
    }
"""

# ============================================================================
# 3. Modify execute_command to handle transaction queueing
# ============================================================================

"""
async def execute_command(self, connection, cmd, args):
    '''Execute a single command with transaction support.'''

    # Check if in transaction mode - queue instead of execute
    queued = self.transaction_mgr.queue_command(connection, cmd, args)
    if queued is not None:
        return queued  # Returns RESP_QUEUED or error

    # Special handling for EXEC - needs access to command router
    if cmd == b'EXEC':
        return self.transaction_mgr.exec(connection, self)

    # Normal command execution
    if cmd not in self.command_table:
        return ERR_UNKNOWN_CMD

    handler, min_arity, max_arity = self.command_table[cmd]

    # Arity check
    arg_count = len(args)
    if min_arity >= 0 and arg_count < min_arity:
        return ERR_WRONG_ARITY
    if max_arity >= 0 and arg_count > max_arity:
        return ERR_WRONG_ARITY

    try:
        return handler(connection, *args)
    except Exception as e:
        return error(f'ERR {str(e)}')
"""

# ============================================================================
# 4. Add command handlers for transaction commands
# ============================================================================

"""
def _cmd_watch(self, connection, *keys):
    '''WATCH key [key ...] - Mark keys for optimistic locking.'''
    return self.transaction_mgr.watch(connection, *keys)


def _cmd_unwatch(self, connection):
    '''UNWATCH - Clear all watched keys.'''
    return self.transaction_mgr.unwatch(connection)


def _cmd_multi(self, connection):
    '''MULTI - Enter transaction mode.'''
    return self.transaction_mgr.multi(connection)


def _cmd_exec(self, connection):
    '''
    EXEC - Execute transaction.

    Note: Actual execution happens in execute_command to access router.
    This handler should never be called directly.
    '''
    # This is handled specially in execute_command
    return error('ERR EXEC handled in execute_command')


def _cmd_discard(self, connection):
    '''DISCARD - Abort transaction.'''
    return self.transaction_mgr.discard(connection)
"""

# ============================================================================
# 5. Add cleanup on client disconnect
# ============================================================================

"""
async def handle_client(self, connection):
    '''Handle client connection lifecycle.'''
    try:
        while connection.is_connected():
            # Read command
            cmd, args = await connection.read_command()

            # Execute command
            response = await self.execute_command(connection, cmd, args)

            # Write response
            await connection.write_response(response)

    except Exception as e:
        print(f'[Error] Client {connection.addr}: {e}')

    finally:
        # Clean up transaction state on disconnect
        self.transaction_mgr.cleanup_client(connection)  # <-- ADD THIS

        await connection.close()
"""

# ============================================================================
# 6. Example: Modified execute method for EXEC
# ============================================================================

"""
# The execute method is called by transaction_mgr.exec()
# It must accept (cmd, args, connection) and return RESP2 response

def execute(self, cmd, args, connection):
    '''
    Execute a single command (called by transaction EXEC).

    Args:
        cmd: bytes - Command name (uppercase)
        args: list - Command arguments
        connection: ClientConnection - Connection context

    Returns:
        bytes: RESP2 encoded response
    '''
    if cmd not in self.command_table:
        return ERR_UNKNOWN_CMD

    handler, min_arity, max_arity = self.command_table[cmd]

    # Arity check
    arg_count = len(args)
    if min_arity >= 0 and arg_count < min_arity:
        return ERR_WRONG_ARITY
    if max_arity >= 0 and arg_count > max_arity:
        return ERR_WRONG_ARITY

    try:
        return handler(connection, *args)
    except Exception as e:
        return error(f'ERR {str(e)}')
"""

# ============================================================================
# 7. Full integration test
# ============================================================================

if __name__ == '__main__':
    print("Transaction Integration Pattern")
    print("=" * 70)
    print("\nThis file shows the integration pattern for TransactionManager.")
    print("\nKey integration points:")
    print("  1. Add TransactionManager to ConnectionHandler.__init__")
    print("  2. Add WATCH/UNWATCH/MULTI/EXEC/DISCARD to command table")
    print("  3. Modify execute_command to check queue_command first")
    print("  4. Handle EXEC specially to pass command router")
    print("  5. Clean up transaction state on disconnect")
    print("  6. Provide execute(cmd, args, connection) method for EXEC")
    print("\nMemory impact:")
    print("  - TransactionManager: ~48 bytes (2 slots)")
    print("  - TransactionState per client: ~96 bytes (4 slots)")
    print("  - Only allocated when client uses WATCH/MULTI")
    print("  - Cleaned up on disconnect")
    print("\nSee examples/transaction_example.py for usage examples.")
    print("=" * 70)

"""
Test suite for MicroRedis Transaction Module

Tests WATCH/MULTI/EXEC/DISCARD with optimistic locking.
Verifies Redis-compatible transaction semantics and version tracking.
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from microredis.storage.engine import Storage
from microredis.features.transaction import TransactionManager, TransactionState
from microredis.core.response import RESP_OK, RESP_QUEUED, RESP_NULL_ARRAY, error, array, integer


class MockConnection:
    """Mock connection object for testing."""
    def __init__(self, id):
        self.id = id


class MockCommandRouter:
    """Mock command router for testing EXEC."""
    def __init__(self, storage):
        self.storage = storage
        self.executed_commands = []

    def execute(self, cmd, args, connection):
        """Execute command and track for verification."""
        self.executed_commands.append((cmd, args))

        # Simple SET/GET implementation
        if cmd == b'SET':
            self.storage.set(args[0], args[1])
            return RESP_OK
        elif cmd == b'GET':
            value = self.storage.get(args[0])
            if value is None:
                return b'$-1\r\n'
            return f'${len(value)}\r\n{value.decode()}\r\n'.encode()
        elif cmd == b'INCR':
            # Simple increment
            val = self.storage.get(args[0])
            if val is None:
                new_val = 1
            else:
                new_val = int(val.decode()) + 1
            self.storage.set(args[0], str(new_val).encode())
            return integer(new_val)

        return RESP_OK


def test_transaction_state():
    """Test TransactionState initialization and reset."""
    print("Testing TransactionState...")

    state = TransactionState()
    assert state.in_multi == False
    assert len(state.command_queue) == 0
    assert len(state.watched_keys) == 0
    assert state.error_state == False

    # Modify state
    state.in_multi = True
    state.command_queue.append((b'SET', [b'key', b'value']))
    state.watched_keys[b'key'] = 1
    state.error_state = True

    # Reset
    state.reset()
    assert state.in_multi == False
    assert len(state.command_queue) == 0
    assert len(state.watched_keys) == 0
    assert state.error_state == False

    print("[OK] TransactionState")


def test_watch():
    """Test WATCH command - records key versions."""
    print("\nTesting WATCH...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn = MockConnection(1)

    # Set some keys
    storage.set(b'key1', b'value1')
    storage.set(b'key2', b'value2')

    # WATCH keys
    result = txn.watch(conn, b'key1', b'key2')
    assert result == RESP_OK

    # Verify versions recorded
    state = txn._client_state[conn]
    assert b'key1' in state.watched_keys
    assert b'key2' in state.watched_keys
    assert state.watched_keys[b'key1'] == storage._version.get(b'key1', 0)

    # WATCH non-existent key
    result = txn.watch(conn, b'nonexistent')
    assert result == RESP_OK
    assert state.watched_keys[b'nonexistent'] == 0

    print("[OK] WATCH OK")


def test_watch_inside_multi_forbidden():
    """Test that WATCH inside MULTI returns error."""
    print("\nTesting WATCH inside MULTI...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn = MockConnection(1)

    # Enter MULTI
    txn.multi(conn)

    # Try WATCH - should fail
    result = txn.watch(conn, b'key')
    assert result.startswith(b'-ERR')
    assert b'WATCH inside MULTI' in result

    print("[OK] WATCH inside MULTI forbidden OK")


def test_unwatch():
    """Test UNWATCH command."""
    print("\nTesting UNWATCH...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn = MockConnection(1)

    # WATCH keys
    storage.set(b'key1', b'value1')
    txn.watch(conn, b'key1')

    state = txn._client_state[conn]
    assert len(state.watched_keys) > 0

    # UNWATCH
    result = txn.unwatch(conn)
    assert result == RESP_OK
    assert len(state.watched_keys) == 0

    # UNWATCH without WATCH is OK
    result = txn.unwatch(conn)
    assert result == RESP_OK

    print("[OK] UNWATCH OK")


def test_multi():
    """Test MULTI command."""
    print("\nTesting MULTI...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn = MockConnection(1)

    # MULTI
    result = txn.multi(conn)
    assert result == RESP_OK

    state = txn._client_state[conn]
    assert state.in_multi == True
    assert len(state.command_queue) == 0
    assert state.error_state == False

    print("[OK] MULTI OK")


def test_nested_multi_forbidden():
    """Test that nested MULTI returns error."""
    print("\nTesting nested MULTI...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn = MockConnection(1)

    # First MULTI
    txn.multi(conn)

    # Second MULTI - should fail
    result = txn.multi(conn)
    assert result.startswith(b'-ERR')
    assert b'nested' in result or b'can not be nested' in result

    print("[OK] Nested MULTI forbidden OK")


def test_queue_command():
    """Test command queueing in MULTI mode."""
    print("\nTesting command queueing...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn = MockConnection(1)

    # Not in MULTI - should return None
    result = txn.queue_command(conn, b'SET', [b'key', b'value'])
    assert result is None

    # Enter MULTI
    txn.multi(conn)

    # Queue commands
    result = txn.queue_command(conn, b'SET', [b'key1', b'value1'])
    assert result == RESP_QUEUED

    result = txn.queue_command(conn, b'GET', [b'key1'])
    assert result == RESP_QUEUED

    result = txn.queue_command(conn, b'INCR', [b'counter'])
    assert result == RESP_QUEUED

    # Verify queue
    state = txn._client_state[conn]
    assert len(state.command_queue) == 3
    assert state.command_queue[0] == (b'SET', [b'key1', b'value1'])
    assert state.command_queue[1] == (b'GET', [b'key1'])
    assert state.command_queue[2] == (b'INCR', [b'counter'])

    print("[OK] Command queueing OK")


def test_queue_forbidden_commands():
    """Test that WATCH/MULTI are forbidden inside MULTI."""
    print("\nTesting forbidden commands in MULTI...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn = MockConnection(1)

    txn.multi(conn)

    # Try WATCH - should fail and set error_state
    result = txn.queue_command(conn, b'WATCH', [b'key'])
    assert result.startswith(b'-ERR')
    assert b'WATCH inside MULTI' in result or b'not allowed' in result

    state = txn._client_state[conn]
    assert state.error_state == True

    # Reset for next test
    state.error_state = False

    # Try MULTI - should fail
    result = txn.queue_command(conn, b'MULTI', [])
    assert result.startswith(b'-ERR')
    assert b'MULTI inside MULTI' in result or b'not allowed' in result

    print("[OK] Forbidden commands in MULTI OK")


def test_exec_without_multi():
    """Test EXEC without MULTI returns error."""
    print("\nTesting EXEC without MULTI...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn = MockConnection(1)
    router = MockCommandRouter(storage)

    # EXEC without MULTI
    result = txn.exec(conn, router)
    assert result.startswith(b'-ERR')
    assert b'EXEC without MULTI' in result

    print("[OK] EXEC without MULTI error OK")


def test_exec_success():
    """Test successful transaction execution."""
    print("\nTesting EXEC success...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn = MockConnection(1)
    router = MockCommandRouter(storage)

    # MULTI and queue commands
    txn.multi(conn)
    txn.queue_command(conn, b'SET', [b'key1', b'value1'])
    txn.queue_command(conn, b'SET', [b'key2', b'value2'])
    txn.queue_command(conn, b'GET', [b'key1'])

    # EXEC
    result = txn.exec(conn, router)

    # Should return array
    assert result.startswith(b'*3\r\n')  # Array of 3 results

    # Verify commands were executed
    assert len(router.executed_commands) == 3
    assert router.executed_commands[0] == (b'SET', [b'key1', b'value1'])

    # Verify keys are set
    assert storage.get(b'key1') == b'value1'
    assert storage.get(b'key2') == b'value2'

    # Verify state reset
    assert conn not in txn._client_state or not txn._client_state[conn].in_multi

    print("[OK] EXEC success OK")


def test_exec_watch_conflict():
    """Test EXEC aborts on WATCH conflict."""
    print("\nTesting EXEC with WATCH conflict...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn = MockConnection(1)
    router = MockCommandRouter(storage)

    # Set initial key
    storage.set(b'counter', b'10')

    # WATCH the key
    txn.watch(conn, b'counter')

    # Record version
    state = txn._client_state[conn]
    initial_version = state.watched_keys[b'counter']

    # Another client modifies the key (simulated)
    storage.set(b'counter', b'20')  # This increments version

    # Verify version changed
    assert storage._version[b'counter'] != initial_version

    # MULTI and queue commands
    txn.multi(conn)
    txn.queue_command(conn, b'INCR', [b'counter'])

    # EXEC - should abort due to version mismatch
    result = txn.exec(conn, router)

    # Should return null array
    assert result == RESP_NULL_ARRAY

    # Verify command was NOT executed
    assert len(router.executed_commands) == 0
    assert storage.get(b'counter') == b'20'  # Still the modified value

    # Verify state reset
    assert len(state.watched_keys) == 0
    assert state.in_multi == False

    print("[OK] EXEC WATCH conflict OK")


def test_exec_with_error_state():
    """Test EXEC returns EXECABORT if error occurred during MULTI."""
    print("\nTesting EXEC with error state...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn = MockConnection(1)
    router = MockCommandRouter(storage)

    # MULTI
    txn.multi(conn)

    # Queue valid command
    txn.queue_command(conn, b'SET', [b'key', b'value'])

    # Try forbidden command (sets error_state)
    txn.queue_command(conn, b'WATCH', [b'key'])

    # EXEC - should return EXECABORT
    result = txn.exec(conn, router)

    assert result.startswith(b'-')
    assert b'EXECABORT' in result

    # Commands should NOT be executed
    assert len(router.executed_commands) == 0

    print("[OK] EXEC with error state OK")


def test_discard():
    """Test DISCARD command."""
    print("\nTesting DISCARD...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn = MockConnection(1)

    # DISCARD without MULTI
    result = txn.discard(conn)
    assert result.startswith(b'-ERR')
    assert b'DISCARD without MULTI' in result

    # MULTI and queue
    txn.multi(conn)
    txn.queue_command(conn, b'SET', [b'key', b'value'])

    state = txn._client_state[conn]
    assert len(state.command_queue) > 0

    # DISCARD
    result = txn.discard(conn)
    assert result == RESP_OK

    # Verify state reset
    assert state.in_multi == False
    assert len(state.command_queue) == 0

    print("[OK] DISCARD OK")


def test_cleanup_client():
    """Test cleanup on client disconnect."""
    print("\nTesting cleanup_client...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn = MockConnection(1)

    # Create state
    txn.watch(conn, b'key')
    txn.multi(conn)
    txn.queue_command(conn, b'SET', [b'key', b'value'])

    assert conn in txn._client_state

    # Cleanup
    txn.cleanup_client(conn)

    assert conn not in txn._client_state

    # Cleanup non-existent client is OK
    txn.cleanup_client(conn)

    print("[OK] cleanup_client OK")


def test_is_in_transaction():
    """Test is_in_transaction helper."""
    print("\nTesting is_in_transaction...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn = MockConnection(1)

    # Not in transaction
    assert txn.is_in_transaction(conn) == False

    # WATCH doesn't set in_multi
    txn.watch(conn, b'key')
    assert txn.is_in_transaction(conn) == False

    # MULTI sets in_multi
    txn.multi(conn)
    assert txn.is_in_transaction(conn) == True

    # DISCARD clears
    txn.discard(conn)
    assert txn.is_in_transaction(conn) == False

    print("[OK] is_in_transaction OK")


def test_multi_client_isolation():
    """Test that multiple clients have isolated transaction state."""
    print("\nTesting multi-client isolation...")

    storage = Storage()
    txn = TransactionManager(storage)
    conn1 = MockConnection(1)
    conn2 = MockConnection(2)

    # Client 1: WATCH and MULTI
    storage.set(b'shared_key', b'v1')
    txn.watch(conn1, b'shared_key')
    txn.multi(conn1)
    txn.queue_command(conn1, b'SET', [b'shared_key', b'v2'])

    # Client 2: MULTI independently
    txn.multi(conn2)
    txn.queue_command(conn2, b'SET', [b'other_key', b'v3'])

    # Verify isolation
    state1 = txn._client_state[conn1]
    state2 = txn._client_state[conn2]

    assert state1 is not state2
    assert len(state1.watched_keys) > 0
    assert len(state2.watched_keys) == 0
    assert state1.command_queue[0][1][0] == b'shared_key'
    assert state2.command_queue[0][1][0] == b'other_key'

    print("[OK] Multi-client isolation OK")


def run_all_tests():
    """Run all transaction tests."""
    print("=" * 60)
    print("MicroRedis Transaction Module Tests")
    print("=" * 60)

    test_transaction_state()
    test_watch()
    test_watch_inside_multi_forbidden()
    test_unwatch()
    test_multi()
    test_nested_multi_forbidden()
    test_queue_command()
    test_queue_forbidden_commands()
    test_exec_without_multi()
    test_exec_success()
    test_exec_watch_conflict()
    test_exec_with_error_state()
    test_discard()
    test_cleanup_client()
    test_is_in_transaction()
    test_multi_client_isolation()

    print("\n" + "=" * 60)
    print("All transaction tests passed!")
    print("=" * 60)


if __name__ == '__main__':
    run_all_tests()

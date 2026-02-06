"""
MicroRedis Transaction Example

Demonstrates WATCH/MULTI/EXEC/DISCARD usage with optimistic locking.
Shows how to integrate TransactionManager with ConnectionHandler.

Usage:
    python examples/transaction_example.py
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from microredis.storage.engine import Storage
from microredis.features.transaction import TransactionManager
from microredis.core.response import RESP_OK, RESP_QUEUED, RESP_NULL_ARRAY


class MockConnection:
    """Simulated client connection."""
    def __init__(self, name):
        self.name = name


def example_basic_transaction():
    """Example: Basic transaction with MULTI/EXEC."""
    print("\n=== Example 1: Basic Transaction ===\n")

    storage = Storage()
    txn_mgr = TransactionManager(storage)
    client = MockConnection("client1")

    # Simulate command router
    class Router:
        def execute(self, cmd, args, connection):
            if cmd == b'SET':
                storage.set(args[0], args[1])
                return RESP_OK
            elif cmd == b'GET':
                value = storage.get(args[0])
                return f'${len(value)}\r\n{value.decode()}\r\n'.encode() if value else b'$-1\r\n'

    router = Router()

    print("1. Client enters MULTI mode")
    result = txn_mgr.multi(client)
    print(f"   MULTI -> {result.decode().strip()}")

    print("\n2. Client queues commands")
    result = txn_mgr.queue_command(client, b'SET', [b'account:1', b'100'])
    print(f"   SET account:1 100 -> {result.decode().strip()}")

    result = txn_mgr.queue_command(client, b'SET', [b'account:2', b'200'])
    print(f"   SET account:2 200 -> {result.decode().strip()}")

    result = txn_mgr.queue_command(client, b'GET', [b'account:1'])
    print(f"   GET account:1 -> {result.decode().strip()}")

    print("\n3. Client executes transaction")
    result = txn_mgr.exec(client, router)
    print(f"   EXEC -> {result.decode().strip()}")

    print("\n4. Verify data stored")
    print(f"   account:1 = {storage.get(b'account:1').decode()}")
    print(f"   account:2 = {storage.get(b'account:2').decode()}")


def example_watch_conflict():
    """Example: WATCH detects concurrent modification."""
    print("\n=== Example 2: WATCH Conflict Detection ===\n")

    storage = Storage()
    txn_mgr = TransactionManager(storage)
    client1 = MockConnection("client1")
    client2 = MockConnection("client2")

    class Router:
        def execute(self, cmd, args, connection):
            if cmd == b'INCR':
                val = storage.get(args[0])
                new_val = (int(val.decode()) + 1) if val else 1
                storage.set(args[0], str(new_val).encode())
                return f':{new_val}\r\n'.encode()
            return RESP_OK

    router = Router()

    # Initialize counter
    storage.set(b'counter', b'10')
    print(f"Initial counter value: {storage.get(b'counter').decode()}")

    print("\n1. Client1 watches counter")
    txn_mgr.watch(client1, b'counter')
    print("   WATCH counter -> +OK")

    print("\n2. Client2 modifies counter (concurrent access)")
    storage.set(b'counter', b'15')
    print(f"   SET counter 15 (external modification)")
    print(f"   Counter version changed: {storage._version[b'counter']}")

    print("\n3. Client1 tries to execute transaction")
    txn_mgr.multi(client1)
    print("   MULTI -> +OK")

    txn_mgr.queue_command(client1, b'INCR', [b'counter'])
    print("   INCR counter -> +QUEUED")

    result = txn_mgr.exec(client1, router)
    print(f"   EXEC -> {result.decode().strip()}")

    print("\n4. Transaction aborted due to version conflict")
    print(f"   Counter value unchanged: {storage.get(b'counter').decode()}")
    print("   (Expected: 15, not 16 - transaction was aborted)")


def example_optimistic_locking_pattern():
    """Example: Optimistic locking for bank transfer."""
    print("\n=== Example 3: Optimistic Locking Pattern ===\n")

    storage = Storage()
    txn_mgr = TransactionManager(storage)
    client = MockConnection("client1")

    class Router:
        def execute(self, cmd, args, connection):
            if cmd == b'SET':
                storage.set(args[0], args[1])
                return RESP_OK
            elif cmd == b'GET':
                value = storage.get(args[0])
                return f'${len(value)}\r\n{value.decode()}\r\n'.encode() if value else b'$-1\r\n'

    router = Router()

    # Initialize accounts
    storage.set(b'balance:alice', b'1000')
    storage.set(b'balance:bob', b'500')

    print("Initial state:")
    print(f"  Alice: ${storage.get(b'balance:alice').decode()}")
    print(f"  Bob: ${storage.get(b'balance:bob').decode()}")

    print("\n1. Watch both accounts (optimistic lock)")
    txn_mgr.watch(client, b'balance:alice', b'balance:bob')
    print("   WATCH balance:alice balance:bob -> +OK")

    print("\n2. Read current balances")
    alice_balance = int(storage.get(b'balance:alice').decode())
    bob_balance = int(storage.get(b'balance:bob').decode())
    print(f"   Alice: ${alice_balance}, Bob: ${bob_balance}")

    print("\n3. Calculate transfer (Alice sends $100 to Bob)")
    transfer_amount = 100
    new_alice = alice_balance - transfer_amount
    new_bob = bob_balance + transfer_amount
    print(f"   New Alice: ${new_alice}, New Bob: ${new_bob}")

    print("\n4. Execute transfer atomically")
    txn_mgr.multi(client)
    txn_mgr.queue_command(client, b'SET', [b'balance:alice', str(new_alice).encode()])
    txn_mgr.queue_command(client, b'SET', [b'balance:bob', str(new_bob).encode()])

    result = txn_mgr.exec(client, router)
    print(f"   EXEC -> {result.decode().strip()}")

    print("\n5. Final state:")
    print(f"  Alice: ${storage.get(b'balance:alice').decode()}")
    print(f"  Bob: ${storage.get(b'balance:bob').decode()}")


def example_discard():
    """Example: Abort transaction with DISCARD."""
    print("\n=== Example 4: DISCARD Transaction ===\n")

    storage = Storage()
    txn_mgr = TransactionManager(storage)
    client = MockConnection("client1")

    print("1. Enter MULTI mode")
    txn_mgr.multi(client)
    print("   MULTI -> +OK")

    print("\n2. Queue dangerous commands")
    txn_mgr.queue_command(client, b'SET', [b'important_key', b'wrong_value'])
    print("   SET important_key wrong_value -> +QUEUED")

    txn_mgr.queue_command(client, b'DEL', [b'critical_data'])
    print("   DEL critical_data -> +QUEUED")

    print("\n3. Realize mistake and abort")
    result = txn_mgr.discard(client)
    print(f"   DISCARD -> {result.decode().strip()}")

    print("\n4. Verify no changes were made")
    print(f"   important_key exists: {storage.exists(b'important_key') > 0}")
    print("   (Transaction was safely aborted)")


def example_error_handling():
    """Example: Error during MULTI sets error state."""
    print("\n=== Example 5: Error Handling in Transaction ===\n")

    storage = Storage()
    txn_mgr = TransactionManager(storage)
    client = MockConnection("client1")

    class Router:
        def execute(self, cmd, args, connection):
            return RESP_OK

    router = Router()

    print("1. Enter MULTI mode")
    txn_mgr.multi(client)
    print("   MULTI -> +OK")

    print("\n2. Queue valid command")
    result = txn_mgr.queue_command(client, b'SET', [b'key', b'value'])
    print(f"   SET key value -> {result.decode().strip()}")

    print("\n3. Try forbidden command (WATCH inside MULTI)")
    result = txn_mgr.queue_command(client, b'WATCH', [b'key'])
    print(f"   WATCH key -> {result.decode().strip()}")

    print("\n4. EXEC returns EXECABORT due to error")
    result = txn_mgr.exec(client, router)
    print(f"   EXEC -> {result.decode().strip()}")
    print("   (Transaction discarded due to previous errors)")


def run_all_examples():
    """Run all transaction examples."""
    print("=" * 70)
    print("MicroRedis Transaction Examples")
    print("=" * 70)

    example_basic_transaction()
    example_watch_conflict()
    example_optimistic_locking_pattern()
    example_discard()
    example_error_handling()

    print("\n" + "=" * 70)
    print("Examples completed successfully!")
    print("=" * 70)
    print("\nKey takeaways:")
    print("  - WATCH provides optimistic locking via version tracking")
    print("  - MULTI queues commands for atomic execution")
    print("  - EXEC checks for conflicts before executing")
    print("  - DISCARD safely aborts transactions")
    print("  - Errors during MULTI prevent execution")


if __name__ == '__main__':
    run_all_examples()

"""
MicroRedis Features Module

This module contains advanced Redis features implemented for MicroPython:

Features:
- Pub/Sub: Publish/Subscribe messaging pattern with channel and pattern subscriptions
- Transactions: MULTI/EXEC/DISCARD transaction support with optimistic locking (WATCH)

These features enable advanced use cases:
- Pub/Sub: Real-time event broadcasting between IoT devices
- Transactions: Atomic operations and optimistic concurrency control

Available submodules:
- pubsub: Pub/Sub engine for channel-based messaging
- transactions: Transaction management with MULTI/EXEC/WATCH support

Memory Considerations:
- Pub/Sub: Each subscription consumes minimal memory (client reference + channel name)
- Transactions: Command queuing during MULTI uses additional memory per queued command
- On ESP32-S3 with limited RAM, monitor active subscriptions and transaction queue depth

Usage:
    from microredis.features import PubSubEngine, TransactionManager

    # Pub/Sub
    pubsub = PubSubEngine()
    pubsub.subscribe(client, b'sensor:temperature')
    pubsub.publish(b'sensor:temperature', b'23.5')

    # Transactions
    txn_mgr = TransactionManager()
    txn_mgr.multi(client)
    txn_mgr.queue_command(client, 'SET', [b'key', b'value'])
    results = await txn_mgr.exec(client, storage)
"""

# Note: Actual implementations will be imported once pubsub.py and transactions.py are created
# For now, this provides the module structure and documentation

__all__ = [
    # Will export: 'PubSubEngine', 'TransactionManager', etc.
]

# Platform compatibility note
try:
    import uasyncio as asyncio
except ImportError:
    import asyncio

__version__ = '1.0.0'

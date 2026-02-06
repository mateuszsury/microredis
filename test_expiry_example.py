"""
Example usage of ExpiryManager module.

This demonstrates how ExpiryManager integrates with Storage
for active TTL management on MicroPython/ESP32-S3.
"""

import time
try:
    import uasyncio as asyncio
except ImportError:
    import asyncio

from microredis.storage.engine import Storage
from microredis.storage.expiry import ExpiryManager


async def main():
    """Demonstrate ExpiryManager usage."""

    # Initialize storage and expiry manager
    storage = Storage()
    expiry_mgr = ExpiryManager(storage)

    print("[ExpiryManager Test] Starting...")

    # Set some keys with TTL
    print("\n1. Setting keys with TTL...")
    storage.set(b'key1', b'value1', px=1000)  # 1 second
    storage.set(b'key2', b'value2', px=2000)  # 2 seconds
    storage.set(b'key3', b'value3', px=3000)  # 3 seconds

    # Add to expiry heap
    for key in [b'key1', b'key2', b'key3']:
        if key in storage._expires:
            expiry_mgr.add_expiry(key, storage._expires[key])

    print(f"   Keys in storage: {len(storage._data)}")
    print(f"   Heap size: {len(expiry_mgr._heap)}")

    # Check nearest expiry
    nearest = expiry_mgr.get_nearest_expiry()
    print(f"   Nearest expiry in: {nearest}ms")

    # Wait 1.5 seconds
    print("\n2. Waiting 1.5 seconds...")
    await asyncio.sleep_ms(1500)

    # Run expiry check
    print("\n3. Running expiry check...")
    deleted = expiry_mgr.expire_keys()
    print(f"   Deleted {deleted} expired keys")
    print(f"   Keys remaining: {storage.exists(b'key1', b'key2', b'key3')}")

    # Check what's left
    print("\n4. Checking individual keys...")
    for key in [b'key1', b'key2', b'key3']:
        value = storage.get(key)
        status = "exists" if value else "expired"
        print(f"   {key.decode()}: {status}")

    # Update TTL for key2
    print("\n5. Updating TTL for key2 to 5 seconds...")
    storage.pexpire(b'key2', 5000)
    expiry_mgr.update_expiry(b'key2', storage._expires[b'key2'])

    # Wait for key3 to expire
    print("\n6. Waiting 2 more seconds...")
    await asyncio.sleep_ms(2000)

    deleted = expiry_mgr.expire_keys()
    print(f"   Deleted {deleted} expired keys")

    # Final check
    print("\n7. Final status:")
    for key in [b'key1', b'key2', b'key3']:
        value = storage.get(key)
        status = "exists" if value else "expired"
        ttl = storage.pttl(key) if value else "N/A"
        print(f"   {key.decode()}: {status} (TTL: {ttl}ms)")

    print("\n[ExpiryManager Test] Complete!")


if __name__ == '__main__':
    # Run the async demo
    try:
        asyncio.run(main())
    except AttributeError:
        # MicroPython doesn't have asyncio.run()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())

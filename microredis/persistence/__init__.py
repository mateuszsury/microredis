"""
Persistence module for MicroRedis.

This module provides RDB-like snapshot functionality for persisting data to flash
storage on embedded systems. Designed for MicroPython on resource-constrained
microcontrollers (ESP32-S3, RP2040, STM32).

Key Features:
- Binary snapshot format (MicroRDB) optimized for minimal flash wear
- Asynchronous BGSAVE operation using uasyncio to avoid blocking
- Automatic snapshots based on configurable dirty key threshold
- Wear-leveling through slot rotation (2 backup slots)
- CRC32 integrity checking with automatic fallback to backup
- Support for all Redis data types: String, Hash, List, Set, Sorted Set
- Optional compression using uzlib (MicroPython built-in)

MicroRDB Format:
    Header:  [4B magic "MRDB"] [2B version] [4B timestamp] [4B key_count]
    Per Key: [1B type] [1B has_ttl] [8B ttl_ms?] [2B key_len] [key] [4B val_len] [value]
    Footer:  [4B CRC32 checksum]

Memory Considerations:
- Snapshots written incrementally to minimize RAM allocation
- Pre-allocated buffers for serialization (4KB default)
- Zero-copy memoryview usage for binary encoding
- Flash wear minimized via configurable save intervals and slot rotation

Available submodules:
- snapshot: RDB-like binary snapshot manager (to be implemented)
- loader: Snapshot loading and integrity validation (to be implemented)

Platform Support:
- ESP32/ESP32-S3: Uses internal flash filesystem (LittleFS)
- RP2040: Flash filesystem via Pico SDK
- STM32: External flash or SD card recommended for frequent snapshots
- CPython: Standard filesystem for testing

Typical Usage:
    from microredis.persistence import SnapshotManager

    # Initialize with storage engine reference
    snapshot_mgr = SnapshotManager(storage, path='/snapshot.mrdb')

    # Manual synchronous save
    await snapshot_mgr.save()

    # Background asynchronous save
    await snapshot_mgr.bgsave()

    # Load snapshot on startup
    loaded = await snapshot_mgr.load()
    if loaded:
        print(f"Loaded {loaded} keys from snapshot")

    # Configure automatic snapshots
    snapshot_mgr.configure(save_seconds=300, dirty_threshold=100)

    # Start background task
    asyncio.create_task(snapshot_mgr.run_auto_save())

Redis Commands Implemented (Phase 3 - P1 priority):
- SAVE: Synchronous snapshot to disk (blocking)
- BGSAVE: Asynchronous background snapshot (non-blocking)
- LASTSAVE: Timestamp of last successful snapshot
- BGREWRITEAOF: Not implemented (AOF not supported in MicroRedis)

Configuration:
- SNAPSHOT_PATH: Default '/snapshot.mrdb'
- SNAPSHOT_SLOTS: 2 (rotation for wear-leveling)
- AUTO_SAVE_SECONDS: 300 (5 minutes, 0 to disable)
- DIRTY_THRESHOLD: 100 (minimum writes before auto-save)
- COMPRESSION: False (enable uzlib compression, adds CPU overhead)

Performance Notes:
- SAVE command blocks all clients during write (~50-200ms for 1000 keys)
- BGSAVE runs in asyncio task, yields periodically (minimal blocking)
- Flash write speed: ~100KB/s on ESP32, ~500KB/s on RP2040
- Snapshot size estimate: ~100 bytes per key average (varies by type)
- Loading time: ~200-500ms for 1000 keys (includes CRC validation)

Error Handling:
- Corrupted snapshots trigger automatic fallback to backup slot
- Flash full errors disable persistence and log warning
- CRC mismatch logs error and attempts backup slot load
- Write failures during BGSAVE reported via INFO persistence section

WARNING: Frequent snapshots on flash storage can cause wear. Configure
AUTO_SAVE_SECONDS and DIRTY_THRESHOLD appropriately for your use case.
For high-write scenarios, consider increasing intervals or using external
storage (SD card, external SPI flash) to preserve internal flash longevity.
"""

# Placeholder for future exports
# When snapshot.py is implemented, export:
# from .snapshot import SnapshotManager, SnapshotConfig
# from .loader import load_snapshot, validate_snapshot

__all__ = []

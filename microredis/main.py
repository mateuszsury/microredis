"""
MicroRedis Main Entry Point

Entry point for MicroRedis server on ESP32-S3 with MicroPython.
Implements the main server class, WiFi helper, and convenience runner.

Platform: ESP32-S3 with MicroPython
Target: ~512KB RAM total, ~300KB available for application
Network: WiFi-capable, port 6379 (standard Redis)

Memory optimizations:
- __slots__ on MicroRedisServer to minimize instance size
- Pre-configured GC threshold for frequent, short collections
- Periodic memory monitoring with automatic gc.collect() when low
- Print-based logging (no logging module overhead)

Usage:
    # On ESP32-S3 with WiFi
    from microredis.main import run, connect_wifi
    connect_wifi('SSID', 'password')
    run()

    # Or directly
    from microredis.main import MicroRedisServer
    server = MicroRedisServer('0.0.0.0', 6379)
    asyncio.run(server.start())
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

import gc
import time

from microredis.storage.engine import Storage
from microredis.storage.expiry import ExpiryManager
from microredis.network.connection import ConnectionHandler
from microredis.network.router import CommandRouter
from microredis.features.pubsub import PubSubManager
from microredis.features.transaction import TransactionManager
from microredis.persistence.snapshot import SnapshotManager
from microredis.config import Config
from microredis.core.constants import DEFAULT_PORT, MAX_CLIENTS

# Memory monitoring constants
MEMORY_CHECK_INTERVAL_S = const(60)  # Check memory every 60 seconds
LOW_MEMORY_THRESHOLD_KB = const(50)  # Force GC if less than 50KB free


class MicroRedisServer:
    """
    Main MicroRedis server implementation.

    Manages the TCP server lifecycle, storage engine, connection handler,
    and periodic memory monitoring for ESP32-S3 deployment.

    Memory optimizations:
    - __slots__ to reduce instance overhead
    - Single Storage instance shared across all connections
    - Single ConnectionHandler for command dispatch
    - Periodic memory monitoring with automatic cleanup
    """

    __slots__ = (
        'host',            # str: Host to bind to (e.g., '0.0.0.0')
        'port',            # int: Port to listen on (default 6379)
        'storage',         # Storage: Shared storage engine instance
        'handler',         # ConnectionHandler: Command handler instance
        'server',          # asyncio.Server: TCP server instance
        '_running',        # bool: Server running flag
        '_client_count',   # int: Current number of connected clients
        '_config',         # Config: Server configuration
        '_router',         # CommandRouter: Command dispatch router
        '_pubsub',         # PubSubManager: Pub/Sub manager
        '_transactions',   # TransactionManager: Transaction manager
        '_expiry_manager', # ExpiryManager: Active expiry manager
        '_snapshot',       # SnapshotManager: Persistence manager
    )

    def __init__(self, host='0.0.0.0', port=DEFAULT_PORT, config=None):
        """
        Initialize MicroRedis server.

        Args:
            host: str - Host address to bind to (default: '0.0.0.0')
            port: int - Port to listen on (default: 6379)
            config: dict - Optional configuration overrides
        """
        self.host = host
        self.port = port

        # Initialize configuration
        self._config = Config(config)

        # Initialize storage engine
        self.storage = Storage()

        # Initialize expiry manager for active TTL expiration
        self._expiry_manager = ExpiryManager(self.storage)
        self.storage.set_expiry_manager(self._expiry_manager)

        # Initialize command router with storage
        self._router = CommandRouter(self.storage)

        # Initialize pub/sub manager
        self._pubsub = PubSubManager()

        # Initialize transaction manager
        self._transactions = TransactionManager(self.storage)

        # Initialize snapshot manager for persistence
        snapshot_file = self._config.get('dbfilename', 'microredis.mrdb')
        self._snapshot = SnapshotManager(
            self.storage,
            filepath=snapshot_file,
            save_interval=300,
            min_changes=100
        )

        # Initialize connection handler with all managers
        self.handler = ConnectionHandler(
            storage=self.storage,
            router=self._router,
            pubsub_manager=self._pubsub,
            transaction_manager=self._transactions,
            config=self._config
        )

        # Server state
        self.server = None
        self._running = False
        self._client_count = 0

        # Configure garbage collector for ESP32-S3
        # Threshold of 32KB triggers frequent, short collections
        # This prevents long GC pauses that could affect connection handling
        try:
            gc.threshold(32768)  # MicroPython only
        except AttributeError:
            pass  # CPython doesn't have gc.threshold()
        gc.collect()

        print(f'[MicroRedis] Initialized on {host}:{port}')

    async def start(self):
        """
        Start the MicroRedis server.

        Binds to host:port and begins accepting client connections.
        Runs periodic memory monitoring in background.
        Blocks until server is stopped.
        """
        if self._running:
            print('[MicroRedis] Server already running')
            return

        # Configure GC before starting
        gc.collect()

        print(f'[MicroRedis] Starting on {self.host}:{self.port}')

        # Create TCP server
        self.server = await asyncio.start_server(
            self._client_connected,
            self.host,
            self.port
        )

        self._running = True
        print(f'[MicroRedis] Server started on {self.host}:{self.port}')

        # Show initial memory status
        try:
            free_kb = gc.mem_free() // 1024
            print(f'[MicroRedis] Memory: {free_kb}KB free')
        except:
            pass  # mem_free() may not be available on all platforms

        # Load existing snapshot if available
        try:
            if self._snapshot.load():
                print('[MicroRedis] Loaded snapshot from disk')
        except Exception as e:
            print(f'[MicroRedis] No snapshot loaded: {e}')

        # Start background tasks
        # 1. Memory monitoring task
        asyncio.create_task(self._monitor_memory())

        # 2. Active expiry loop - deletes expired keys proactively
        asyncio.create_task(self._expiry_manager.run_expiry_loop())
        print('[MicroRedis] Started active expiry loop')

        # 3. Auto-save snapshot loop - persists data periodically
        asyncio.create_task(self._snapshot.auto_save_loop())
        print('[MicroRedis] Started auto-save loop')

        # Keep server running
        try:
            # Wait indefinitely (or until interrupted)
            while self._running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            print('[MicroRedis] Interrupted by user')
        finally:
            await self.stop()

    async def stop(self):
        """
        Stop the MicroRedis server gracefully.

        Closes the TCP server and waits for pending connections to finish.
        """
        if not self._running:
            return

        print('[MicroRedis] Stopping server...')
        self._running = False

        # Save snapshot before shutdown
        try:
            print('[MicroRedis] Saving final snapshot...')
            self._snapshot.save()
        except Exception as e:
            print(f'[MicroRedis] Failed to save snapshot: {e}')

        if self.server:
            # Close server (stops accepting new connections)
            self.server.close()

            # Wait for server to close
            await self.server.wait_closed()

        print('[MicroRedis] Server stopped')

        # Final garbage collection
        gc.collect()

    async def _client_connected(self, reader, writer):
        """
        Callback for new client connections.

        Args:
            reader: asyncio.StreamReader for socket input
            writer: asyncio.StreamWriter for socket output
        """
        # Get client address
        addr = writer.get_extra_info('peername', ('unknown', 0))

        # Check if we've reached max clients
        if self._client_count >= MAX_CLIENTS:
            print(f'[MicroRedis] Rejected connection (max clients): {addr}')
            try:
                writer.close()
                await writer.wait_closed() if hasattr(writer, 'wait_closed') else None
            except:
                pass
            return

        # Increment client counter
        self._client_count += 1
        print(f'[MicroRedis] Client connected: {addr} ({self._client_count}/{MAX_CLIENTS})')

        try:
            # Show memory status on connect
            try:
                free_kb = gc.mem_free() // 1024
                print(f'[MicroRedis] Memory: {free_kb}KB free')
            except:
                pass

            # Handle client connection (blocking until client disconnects)
            await self.handler.handle_client(reader, writer)

        finally:
            # Decrement client counter
            self._client_count -= 1
            print(f'[MicroRedis] Client disconnected: {addr} ({self._client_count}/{MAX_CLIENTS})')

            # Collect garbage after client disconnect
            gc.collect()

    async def _monitor_memory(self):
        """
        Periodic memory monitoring task.

        Checks memory every 60 seconds and forces garbage collection
        if free memory drops below 50KB.

        This prevents memory fragmentation and OOM errors on ESP32-S3.
        """
        while self._running:
            try:
                # Wait for check interval
                await asyncio.sleep(MEMORY_CHECK_INTERVAL_S)

                # Check memory status (MicroPython-specific)
                try:
                    free_kb = gc.mem_free() // 1024
                    alloc_kb = gc.mem_alloc() // 1024

                    # Log memory status
                    print(f'[MicroRedis] Memory check: {free_kb}KB free, {alloc_kb}KB allocated')

                    # Force GC if low on memory
                    if free_kb < LOW_MEMORY_THRESHOLD_KB:
                        print(f'[MicroRedis] Low memory detected ({free_kb}KB), forcing GC...')
                        gc.collect()

                        # Check again after GC
                        free_kb = gc.mem_free() // 1024
                        print(f'[MicroRedis] After GC: {free_kb}KB free')

                except AttributeError:
                    # mem_free() not available on CPython - skip monitoring
                    pass

            except Exception as e:
                print(f'[MicroRedis] Memory monitor error: {e}')

    def get_info(self):
        """
        Get server statistics.

        Returns:
            dict: Server information including:
                - host: str - bind address
                - port: int - listening port
                - running: bool - server running status
                - clients: int - current client count
                - max_clients: int - maximum allowed clients
                - keys: int - total keys in storage
        """
        # Count keys in storage
        try:
            key_count = len(self.storage._data)
        except:
            key_count = 0

        return {
            'host': self.host,
            'port': self.port,
            'running': self._running,
            'clients': self._client_count,
            'max_clients': MAX_CLIENTS,
            'keys': key_count,
        }


def run(host='0.0.0.0', port=DEFAULT_PORT):
    """
    Convenience function to run MicroRedis server.

    Creates a server instance and runs it until interrupted.
    Handles KeyboardInterrupt gracefully for clean shutdown.

    Args:
        host: str - Host address to bind to (default: '0.0.0.0')
        port: int - Port to listen on (default: 6379)

    Usage:
        from microredis.main import run
        run()  # Starts server on 0.0.0.0:6379
    """
    server = MicroRedisServer(host, port)

    try:
        # Run server
        asyncio.run(server.start())
    except KeyboardInterrupt:
        # Graceful shutdown on Ctrl+C
        print('\n[MicroRedis] Shutting down...')
        asyncio.run(server.stop())


def connect_wifi(ssid, password, timeout=30):
    """
    Connect to WiFi network (ESP32-specific helper).

    This is a convenience function for ESP32-S3 deployment.
    Blocks until connection is established or timeout is reached.

    Args:
        ssid: str - WiFi network SSID
        password: str - WiFi password
        timeout: int - Connection timeout in seconds (default: 30)

    Returns:
        str: IP address assigned to device

    Raises:
        RuntimeError: If connection fails or times out
        ImportError: If network module is not available (not on ESP32)

    Usage:
        from microredis.main import connect_wifi, run
        ip = connect_wifi('MyNetwork', 'password123')
        print(f'Connected with IP: {ip}')
        run()
    """
    try:
        import network
    except ImportError:
        raise ImportError('network module not available (not running on ESP32)')

    # Create WLAN interface
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)

    # Check if already connected
    if wlan.isconnected():
        ip = wlan.ifconfig()[0]
        print(f'[MicroRedis] Already connected to WiFi: {ip}')
        return ip

    # Connect to network
    print(f'[MicroRedis] Connecting to WiFi: {ssid}')
    wlan.connect(ssid, password)

    # Wait for connection with timeout
    start_time = time.time()
    while not wlan.isconnected():
        if time.time() - start_time > timeout:
            raise RuntimeError(f'WiFi connection timeout after {timeout}s')

        # Small delay to avoid busy-waiting
        time.sleep(0.1)

    # Get IP address
    ip = wlan.ifconfig()[0]
    print(f'[MicroRedis] WiFi connected: {ip}')

    return ip


# Entry point for `python -m microredis`
if __name__ == '__main__':
    run()

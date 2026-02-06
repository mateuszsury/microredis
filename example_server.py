"""
Example MicroRedis server implementation.

Demonstrates how to use the connection handler with the storage engine
to create a basic Redis-compatible server.

Usage:
    python example_server.py

Then connect with redis-cli:
    redis-cli -p 6379
"""

try:
    import uasyncio as asyncio
except ImportError:
    import asyncio

from microredis.storage.engine import Storage
from microredis.network.connection import ConnectionHandler
from microredis.core.constants import DEFAULT_PORT, MAX_CLIENTS


async def start_server(host='0.0.0.0', port=DEFAULT_PORT):
    """
    Start the MicroRedis server.

    Args:
        host: IP address to bind to (default: 0.0.0.0 - all interfaces)
        port: Port to listen on (default: 6379)
    """
    # Create storage engine
    storage = Storage()

    # Create connection handler
    handler = ConnectionHandler(storage)

    # Start TCP server
    server = await asyncio.start_server(
        handler.handle_client,
        host,
        port
    )

    addr = server.sockets[0].getsockname() if hasattr(server, 'sockets') else (host, port)
    print(f"MicroRedis server started on {addr[0]}:{addr[1]}")
    print(f"Maximum clients: {MAX_CLIENTS}")
    print(f"Ready to accept connections...")

    # Run server forever
    async with server:
        await server.serve_forever()


async def main():
    """Main entry point."""
    try:
        await start_server()
    except KeyboardInterrupt:
        print("\nShutting down server...")
    except Exception as e:
        print(f"Server error: {e}")
        raise


if __name__ == '__main__':
    # Run the server
    asyncio.run(main())

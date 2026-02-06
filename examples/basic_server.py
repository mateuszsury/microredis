"""
Basic MicroRedis Server Example

Demonstrates how to run a MicroRedis server on ESP32-S3 or local machine.

Platform: ESP32-S3 with MicroPython (also works on CPython for testing)
"""

# Option 1: Quick start with defaults
# Runs on 0.0.0.0:6379
def quick_start():
    """Start server with default settings (0.0.0.0:6379)."""
    from microredis.main import run
    run()


# Option 2: Custom host/port
def custom_server():
    """Start server with custom host and port."""
    from microredis.main import run
    run(host='127.0.0.1', port=16379)


# Option 3: ESP32-S3 with WiFi
def esp32_server():
    """Start server on ESP32-S3 with WiFi connection."""
    from microredis.main import connect_wifi, run

    # Connect to WiFi first
    ip = connect_wifi('YourSSID', 'YourPassword')
    print(f'Server will be accessible at {ip}:6379')

    # Start server
    run()


# Option 4: Programmatic control with server instance
async def programmatic_server():
    """Start server with programmatic control."""
    try:
        import uasyncio as asyncio
    except ImportError:
        import asyncio

    from microredis.main import MicroRedisServer

    # Create server instance
    server = MicroRedisServer('0.0.0.0', 6379)

    # Get initial info
    info = server.get_info()
    print(f'Server configured: {info}')

    # Start server (blocking)
    await server.start()


if __name__ == '__main__':
    # Choose which example to run:

    # For quick start (most common):
    quick_start()

    # For custom configuration:
    # custom_server()

    # For ESP32-S3:
    # esp32_server()

    # For programmatic control:
    # import asyncio
    # asyncio.run(programmatic_server())

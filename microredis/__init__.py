"""
MicroRedis - A lightweight Redis-compatible key-value store for MicroPython.

MicroRedis provides a minimal Redis server implementation optimized for
resource-constrained microcontrollers running MicroPython. It supports
basic Redis commands and the RESP protocol for communication.

Platform Compatibility:
- ESP32/ESP8266
- Raspberry Pi Pico
- Other MicroPython-compatible boards with network support

Memory Requirements:
- Minimal RAM footprint for embedded systems
- Configurable memory limits for data storage

Usage:
    from microredis import MicroRedisServer

    server = MicroRedisServer(host='0.0.0.0', port=6379)
    server.start()
"""

__version__ = '1.0.0'
__all__ = ['__version__']

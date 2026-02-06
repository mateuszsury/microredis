"""
Test script for connection handler module.
Tests command parsing and response generation.
"""

import asyncio
from io import BytesIO


# Mock StreamReader/Writer for testing
class MockStreamReader:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    async def read(self, n):
        chunk = self.data[self.pos:self.pos + n]
        self.pos += len(chunk)
        await asyncio.sleep(0)  # Yield to event loop
        return chunk


class MockStreamWriter:
    def __init__(self):
        self.buffer = bytearray()
        self.closed = False

    def write(self, data):
        self.buffer.extend(data)

    async def drain(self):
        await asyncio.sleep(0)

    def close(self):
        self.closed = True

    async def wait_closed(self):
        await asyncio.sleep(0)

    def get_extra_info(self, key, default=None):
        if key == 'peername':
            return ('127.0.0.1', 12345)
        return default


# Use real Storage for testing
from microredis.storage.engine import Storage as MockStorage


async def test_ping():
    """Test PING command"""
    from microredis.network.connection import ConnectionHandler

    # Create handler
    storage = MockStorage()
    handler = ConnectionHandler(storage)

    # Test PING without argument
    reader = MockStreamReader(b'*1\r\n$4\r\nPING\r\n')
    writer = MockStreamWriter()

    await handler.handle_client(reader, writer)

    response = bytes(writer.buffer)
    assert response == b'+PONG\r\n', f"Expected PONG, got {response}"
    print("[OK] PING test passed")


async def test_echo():
    """Test ECHO command"""
    from microredis.network.connection import ConnectionHandler

    storage = MockStorage()
    handler = ConnectionHandler(storage)

    # Test ECHO
    reader = MockStreamReader(b'*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n')
    writer = MockStreamWriter()

    await handler.handle_client(reader, writer)

    response = bytes(writer.buffer)
    assert response == b'$5\r\nhello\r\n', f"Expected hello, got {response}"
    print("[OK] ECHO test passed")


async def test_set_get():
    """Test SET and GET commands"""
    from microredis.network.connection import ConnectionHandler

    storage = MockStorage()
    handler = ConnectionHandler(storage)

    # Test SET
    reader = MockStreamReader(b'*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n')
    writer = MockStreamWriter()

    await handler.handle_client(reader, writer)

    response = bytes(writer.buffer)
    assert response == b'+OK\r\n', f"Expected OK, got {response}"
    print("[OK] SET test passed")

    # Test GET
    reader = MockStreamReader(b'*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n')
    writer = MockStreamWriter()

    await handler.handle_client(reader, writer)

    response = bytes(writer.buffer)
    assert response == b'$5\r\nvalue\r\n', f"Expected value, got {response}"
    print("[OK] GET test passed")


async def test_del():
    """Test DEL command"""
    from microredis.network.connection import ConnectionHandler

    storage = MockStorage()
    storage.set(b'key1', b'value1', ex=None, px=None, nx=False, xx=False)
    storage.set(b'key2', b'value2', ex=None, px=None, nx=False, xx=False)
    handler = ConnectionHandler(storage)

    # Test DEL multiple keys
    reader = MockStreamReader(b'*3\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n')
    writer = MockStreamWriter()

    await handler.handle_client(reader, writer)

    response = bytes(writer.buffer)
    assert response == b':2\r\n', f"Expected :2, got {response}"
    print("[OK] DEL test passed")


async def test_exists():
    """Test EXISTS command"""
    from microredis.network.connection import ConnectionHandler

    storage = MockStorage()
    storage.set(b'key1', b'value1', ex=None, px=None, nx=False, xx=False)
    handler = ConnectionHandler(storage)

    # Test EXISTS
    reader = MockStreamReader(b'*3\r\n$6\r\nEXISTS\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n')
    writer = MockStreamWriter()

    await handler.handle_client(reader, writer)

    response = bytes(writer.buffer)
    assert response == b':1\r\n', f"Expected :1, got {response}"
    print("[OK] EXISTS test passed")


async def test_unknown_command():
    """Test unknown command handling"""
    from microredis.network.connection import ConnectionHandler

    storage = MockStorage()
    handler = ConnectionHandler(storage)

    # Test unknown command
    reader = MockStreamReader(b'*1\r\n$7\r\nUNKNOWN\r\n')
    writer = MockStreamWriter()

    await handler.handle_client(reader, writer)

    response = bytes(writer.buffer)
    assert response.startswith(b'-ERR unknown command'), f"Expected error, got {response}"
    print("[OK] Unknown command test passed")


async def main():
    """Run all tests"""
    print("Running connection handler tests...\n")

    await test_ping()
    await test_echo()
    await test_set_get()
    await test_del()
    await test_exists()
    await test_unknown_command()

    print("\nAll tests passed!")


if __name__ == '__main__':
    asyncio.run(main())

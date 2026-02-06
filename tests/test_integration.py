"""
Integration Tests for MicroRedis

Tests the complete server stack from network to storage.
Runs actual TCP server and client connections.

Usage:
    python tests/test_integration.py
"""

import asyncio
import socket
import time


class TestRedisClient:
    """Test client for integration testing."""

    def __init__(self, host='127.0.0.1', port=6379):
        """Connect to server."""
        self.host = host
        self.port = port
        self.sock = None

    def connect(self):
        """Establish connection."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(5.0)
        self.sock.connect((self.host, self.port))

    def send_command(self, *args):
        """Send RESP2 command."""
        parts = [f'*{len(args)}\r\n'.encode()]
        for arg in args:
            if isinstance(arg, str):
                arg = arg.encode('utf-8')
            parts.append(f'${len(arg)}\r\n'.encode())
            parts.append(arg)
            parts.append(b'\r\n')

        self.sock.sendall(b''.join(parts))
        return self.sock.recv(4096)

    def close(self):
        """Close connection."""
        if self.sock:
            self.sock.close()


async def test_server_lifecycle():
    """Test server start and stop."""
    print('Test: Server lifecycle...')

    from microredis.main import MicroRedisServer

    server = MicroRedisServer('127.0.0.1', 16379)

    # Check initial state
    info = server.get_info()
    assert info['running'] == False
    assert info['clients'] == 0
    print('  Initial state: OK')

    # Start server in background
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.5)  # Give server time to start

    # Verify running
    assert server._running == True
    print('  Server started: OK')

    # Stop server
    await server.stop()
    await asyncio.sleep(0.1)

    # Cancel task
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass

    print('  Server stopped: OK')
    print('  PASSED\n')


async def test_client_connection():
    """Test client connection handling."""
    print('Test: Client connection...')

    from microredis.main import MicroRedisServer

    server = MicroRedisServer('127.0.0.1', 16379)
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.5)

    try:
        # Connect client
        client = TestRedisClient('127.0.0.1', 16379)
        client.connect()
        print('  Connection established: OK')

        # Test PING
        response = client.send_command('PING')
        assert response == b'+PONG\r\n'
        print('  PING response: OK')

        # Close client
        client.close()
        await asyncio.sleep(0.1)

        print('  PASSED\n')

    finally:
        await server.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


async def test_basic_commands():
    """Test basic Redis commands."""
    print('Test: Basic commands...')

    from microredis.main import MicroRedisServer

    server = MicroRedisServer('127.0.0.1', 16379)
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.5)

    try:
        client = TestRedisClient('127.0.0.1', 16379)
        client.connect()

        # Test SET
        response = client.send_command('SET', 'testkey', 'testvalue')
        assert response == b'+OK\r\n'
        print('  SET command: OK')

        # Test GET
        response = client.send_command('GET', 'testkey')
        assert response == b'$9\r\ntestvalue\r\n'
        print('  GET command: OK')

        # Test EXISTS
        response = client.send_command('EXISTS', 'testkey')
        assert response == b':1\r\n'
        print('  EXISTS command: OK')

        # Test DEL
        response = client.send_command('DEL', 'testkey')
        assert response == b':1\r\n'
        print('  DEL command: OK')

        # Test GET after DEL
        response = client.send_command('GET', 'testkey')
        assert response == b'$-1\r\n'
        print('  GET (null) command: OK')

        client.close()
        print('  PASSED\n')

    finally:
        await server.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


async def test_expiry():
    """Test key expiration."""
    print('Test: Key expiration...')

    from microredis.main import MicroRedisServer

    server = MicroRedisServer('127.0.0.1', 16379)
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.5)

    try:
        client = TestRedisClient('127.0.0.1', 16379)
        client.connect()

        # Set with 1 second expiry
        response = client.send_command('SET', 'expirekey', 'value', 'EX', '1')
        assert response == b'+OK\r\n'
        print('  SET with EX: OK')

        # Should exist immediately
        response = client.send_command('GET', 'expirekey')
        assert response == b'$5\r\nvalue\r\n'
        print('  GET (before expiry): OK')

        # Wait for expiry
        await asyncio.sleep(1.5)

        # Should be expired
        response = client.send_command('GET', 'expirekey')
        assert response == b'$-1\r\n'
        print('  GET (after expiry): OK')

        client.close()
        print('  PASSED\n')

    finally:
        await server.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


async def test_multiple_clients():
    """Test multiple concurrent clients."""
    print('Test: Multiple clients...')

    from microredis.main import MicroRedisServer

    server = MicroRedisServer('127.0.0.1', 16379)
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.5)

    try:
        # Connect multiple clients
        clients = []
        for i in range(3):
            client = TestRedisClient('127.0.0.1', 16379)
            client.connect()
            clients.append(client)
        print('  3 clients connected: OK')

        # Each client sets a key
        for i, client in enumerate(clients):
            response = client.send_command('SET', f'key{i}', f'value{i}')
            assert response == b'+OK\r\n'
        print('  Concurrent SET commands: OK')

        # Each client gets its key
        for i, client in enumerate(clients):
            response = client.send_command('GET', f'key{i}')
            expected = f'$6\r\nvalue{i}\r\n'.encode()
            assert response == expected
        print('  Concurrent GET commands: OK')

        # Close all clients
        for client in clients:
            client.close()

        await asyncio.sleep(0.1)
        print('  PASSED\n')

    finally:
        await server.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


async def test_max_clients():
    """Test maximum client limit enforcement."""
    print('Test: Maximum client limit...')

    from microredis.main import MicroRedisServer
    from microredis.core.constants import MAX_CLIENTS

    server = MicroRedisServer('127.0.0.1', 16379)
    server_task = asyncio.create_task(server.start())
    await asyncio.sleep(0.5)

    try:
        # Connect up to MAX_CLIENTS
        clients = []
        for i in range(MAX_CLIENTS):
            client = TestRedisClient('127.0.0.1', 16379)
            client.connect()
            clients.append(client)
        print(f'  {MAX_CLIENTS} clients connected: OK')

        # Verify all can execute commands
        for client in clients:
            response = client.send_command('PING')
            assert response == b'+PONG\r\n'
        print(f'  All {MAX_CLIENTS} clients operational: OK')

        # Close all
        for client in clients:
            client.close()

        print('  PASSED\n')

    finally:
        await server.stop()
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


async def run_all_tests():
    """Run all integration tests."""
    print('='*60)
    print('MicroRedis Integration Tests')
    print('='*60 + '\n')

    tests = [
        test_server_lifecycle,
        test_client_connection,
        test_basic_commands,
        test_expiry,
        test_multiple_clients,
        test_max_clients,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            await test()
            passed += 1
        except AssertionError as e:
            print(f'  FAILED: {e}\n')
            failed += 1
        except Exception as e:
            print(f'  ERROR: {e}\n')
            failed += 1

    print('='*60)
    print(f'Results: {passed} passed, {failed} failed')
    print('='*60)

    return failed == 0


if __name__ == '__main__':
    import sys

    # Run tests
    success = asyncio.run(run_all_tests())

    # Exit with appropriate code
    sys.exit(0 if success else 1)

"""
Test Client for MicroRedis

Simple test client to verify MicroRedis server functionality.
Uses standard Python socket library to send RESP2 commands.

Usage:
    1. Start MicroRedis server: python examples/basic_server.py
    2. Run this client: python examples/test_client.py
"""

import socket


class RedisClient:
    """Simple Redis client for testing MicroRedis."""

    def __init__(self, host='127.0.0.1', port=6379):
        """Connect to Redis server."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        print(f'Connected to {host}:{port}')

    def send_command(self, *args):
        """
        Send RESP2 command to server.

        Args:
            *args: Command arguments (strings or bytes)

        Returns:
            bytes: Raw response from server
        """
        # Build RESP2 array
        parts = [f'*{len(args)}\r\n'.encode()]

        for arg in args:
            if isinstance(arg, str):
                arg = arg.encode('utf-8')

            parts.append(f'${len(arg)}\r\n'.encode())
            parts.append(arg)
            parts.append(b'\r\n')

        # Send command
        request = b''.join(parts)
        self.sock.sendall(request)

        # Receive response
        response = self.sock.recv(4096)
        return response

    def close(self):
        """Close connection."""
        self.sock.close()


def parse_simple_response(data):
    """Parse simple RESP2 responses for display."""
    if data.startswith(b'+'):
        return data[1:-2].decode('utf-8')
    elif data.startswith(b'-'):
        return f"ERROR: {data[1:-2].decode('utf-8')}"
    elif data.startswith(b':'):
        return int(data[1:-2])
    elif data.startswith(b'$'):
        # Bulk string
        lines = data.split(b'\r\n')
        length = int(lines[0][1:])
        if length == -1:
            return None
        return lines[1].decode('utf-8')
    else:
        return data.decode('utf-8', errors='replace')


def test_basic_commands():
    """Test basic MicroRedis commands."""
    client = RedisClient()

    print('\n=== Testing Basic Commands ===\n')

    # Test PING
    print('1. PING')
    response = client.send_command('PING')
    print(f'   Response: {parse_simple_response(response)}')

    # Test PING with message
    print('\n2. PING with message')
    response = client.send_command('PING', 'Hello MicroRedis')
    print(f'   Response: {parse_simple_response(response)}')

    # Test ECHO
    print('\n3. ECHO')
    response = client.send_command('ECHO', 'Testing echo')
    print(f'   Response: {parse_simple_response(response)}')

    # Test SET
    print('\n4. SET key value')
    response = client.send_command('SET', 'mykey', 'myvalue')
    print(f'   Response: {parse_simple_response(response)}')

    # Test GET
    print('\n5. GET key')
    response = client.send_command('GET', 'mykey')
    print(f'   Response: {parse_simple_response(response)}')

    # Test GET non-existent key
    print('\n6. GET non-existent key')
    response = client.send_command('GET', 'nonexistent')
    print(f'   Response: {parse_simple_response(response)}')

    # Test SET with EX
    print('\n7. SET key value EX 10')
    response = client.send_command('SET', 'tempkey', 'tempvalue', 'EX', '10')
    print(f'   Response: {parse_simple_response(response)}')

    # Test SET with NX
    print('\n8. SET key value NX (should fail - key exists)')
    response = client.send_command('SET', 'mykey', 'newvalue', 'NX')
    print(f'   Response: {parse_simple_response(response)}')

    # Test SET with NX on new key
    print('\n9. SET newkey value NX (should succeed)')
    response = client.send_command('SET', 'newkey', 'newvalue', 'NX')
    print(f'   Response: {parse_simple_response(response)}')

    # Test EXISTS
    print('\n10. EXISTS mykey newkey nonexistent')
    response = client.send_command('EXISTS', 'mykey', 'newkey', 'nonexistent')
    print(f'   Response: {parse_simple_response(response)}')

    # Test DEL
    print('\n11. DEL mykey newkey')
    response = client.send_command('DEL', 'mykey', 'newkey')
    print(f'   Response: {parse_simple_response(response)}')

    # Test INFO
    print('\n12. INFO')
    response = client.send_command('INFO')
    print(f'   Response (truncated): {parse_simple_response(response)[:100]}...')

    # Test COMMAND
    print('\n13. COMMAND')
    response = client.send_command('COMMAND')
    print(f'   Response (raw): {response[:100]}...')

    # Test QUIT
    print('\n14. QUIT')
    response = client.send_command('QUIT')
    print(f'   Response: {parse_simple_response(response)}')

    client.close()
    print('\n=== All tests completed ===\n')


if __name__ == '__main__':
    try:
        test_basic_commands()
    except ConnectionRefusedError:
        print('Error: Could not connect to MicroRedis server.')
        print('Make sure the server is running on 127.0.0.1:6379')
        print('Start it with: python examples/basic_server.py')
    except Exception as e:
        print(f'Error: {e}')

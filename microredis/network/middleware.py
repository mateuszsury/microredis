"""
Middleware for MicroRedis - Authentication, Rate Limiting, and Request Validation

Optimized for ESP32-S3 with MicroPython:
- Minimal memory allocations
- Pre-allocated error responses
- __slots__ on all classes
- Efficient timestamp tracking with ticks_ms()
"""

try:
    from time import ticks_ms, ticks_diff
except ImportError:
    from time import time
    def ticks_ms():
        return int(time() * 1000)
    def ticks_diff(a, b):
        return a - b

# Pre-allocated error responses (zero allocation at runtime)
ERR_NOAUTH = b'-NOAUTH Authentication required.\r\n'
ERR_INVALID_PASSWORD = b'-WRONGPASS invalid password\r\n'
ERR_REQUEST_TOO_LARGE = b'-ERR request too large\r\n'
ERR_TOO_MANY_ARGS = b'-ERR too many arguments\r\n'
ERR_RATE_LIMIT = b'-ERR rate limit exceeded\r\n'

# Pre-allocated success responses
AUTH_OK = b'+OK\r\n'

# Commands allowed without authentication
NOAUTH_COMMANDS = frozenset((b'AUTH', b'PING', b'QUIT'))


class AuthMiddleware:
    """
    Authentication middleware for Redis protocol.

    Enforces authentication when password is set and tracks auth state per connection.
    """
    __slots__ = ('_password',)

    def __init__(self, password=None):
        """
        Initialize authentication middleware.

        Args:
            password: bytes | str | None - Required password, or None to disable auth
        """
        if password is None:
            self._password = None
        elif isinstance(password, str):
            self._password = password.encode('utf-8')
        elif isinstance(password, bytes):
            self._password = password
        else:
            raise TypeError("Password must be str, bytes, or None")

    def check_auth(self, connection, cmd, args):
        """
        Check if connection is authenticated for the given command.

        Args:
            connection: ClientConnection instance
            cmd: bytes - Command name (uppercase)
            args: list - Command arguments

        Returns:
            tuple[bool, bytes | None] - (success, error_response)
                (True, None) if authenticated or auth not required
                (False, error_response) if authentication required
        """
        # No password configured - all commands allowed
        if self._password is None:
            return (True, None)

        # AUTH, PING, QUIT allowed without authentication
        if cmd in NOAUTH_COMMANDS:
            return (True, None)

        # Check if connection is authenticated
        if not hasattr(connection, 'authenticated') or not connection.authenticated:
            return (False, ERR_NOAUTH)

        return (True, None)

    def handle_auth(self, connection, args):
        """
        Handle AUTH command.

        Args:
            connection: ClientConnection instance
            args: list[bytes] - Command arguments [password]

        Returns:
            bytes - Response (AUTH_OK or error)
        """
        # No password configured
        if self._password is None:
            return b'-ERR Client sent AUTH, but no password is set\r\n'

        # Wrong number of arguments
        if len(args) != 1:
            return b'-ERR wrong number of arguments for \'auth\' command\r\n'

        # Check password
        if args[0] == self._password:
            connection.authenticated = True
            return AUTH_OK
        else:
            connection.authenticated = False
            return ERR_INVALID_PASSWORD


class RequestValidator:
    """
    Request validation middleware.

    Validates request size and argument count to prevent memory exhaustion.
    """
    __slots__ = ('_max_bulk_size', '_max_args')

    def __init__(self, max_bulk_size=1048576, max_args=100):
        """
        Initialize request validator.

        Args:
            max_bulk_size: int - Maximum size of bulk string in bytes (default 1MB for ESP32)
            max_args: int - Maximum number of arguments (default 100 for ESP32)
        """
        self._max_bulk_size = max_bulk_size
        self._max_args = max_args

    def validate_request(self, cmd, args):
        """
        Validate request parameters.

        Args:
            cmd: bytes - Command name
            args: list - Command arguments

        Returns:
            tuple[bool, bytes | None] - (valid, error_response)
                (True, None) if valid
                (False, error_response) if invalid
        """
        # Check argument count
        if len(args) > self._max_args:
            return (False, ERR_TOO_MANY_ARGS)

        # Check bulk string sizes
        for arg in args:
            if isinstance(arg, (bytes, bytearray, memoryview)):
                if len(arg) > self._max_bulk_size:
                    return (False, ERR_REQUEST_TOO_LARGE)

        return (True, None)

    def validate_bulk_size(self, size):
        """
        Validate bulk string size during parsing.

        Args:
            size: int - Declared bulk string size

        Returns:
            bool - True if valid, False if too large
        """
        return size <= self._max_bulk_size


class RateLimiter:
    """
    Simple rate limiter based on sliding window.

    Tracks requests per client address to prevent abuse.
    ESP32-optimized: uses ticks_ms() and prunes old entries.
    """
    __slots__ = ('_requests', '_max_requests', '_window_ms', '_last_prune')

    def __init__(self, max_requests=100, window_ms=1000):
        """
        Initialize rate limiter.

        Args:
            max_requests: int - Maximum requests per window
            window_ms: int - Time window in milliseconds
        """
        self._requests = {}  # {addr: [timestamp, timestamp, ...]}
        self._max_requests = max_requests
        self._window_ms = window_ms
        self._last_prune = ticks_ms()

    def check_rate(self, addr):
        """
        Check if address is within rate limit.

        Args:
            addr: tuple - Client address (host, port)

        Returns:
            bool - True if within limit, False if exceeded
        """
        now = ticks_ms()

        # Prune old entries every 10 seconds to avoid memory growth
        if ticks_diff(now, self._last_prune) > 10000:
            self._prune_old_entries(now)
            self._last_prune = now

        # Get request timestamps for this address
        if addr not in self._requests:
            return True

        timestamps = self._requests[addr]

        # Remove timestamps outside the window
        cutoff = now - self._window_ms
        # Filter in-place to avoid allocation
        i = 0
        while i < len(timestamps):
            if ticks_diff(timestamps[i], cutoff) < 0:
                # Too old, remove it
                timestamps.pop(i)
            else:
                i += 1

        # Check if under limit
        return len(timestamps) < self._max_requests

    def record_request(self, addr):
        """
        Record a request from the given address.

        Args:
            addr: tuple - Client address (host, port)
        """
        now = ticks_ms()

        if addr not in self._requests:
            self._requests[addr] = []

        self._requests[addr].append(now)

    def _prune_old_entries(self, now):
        """
        Remove old timestamp lists to prevent memory growth.

        Args:
            now: int - Current timestamp in milliseconds
        """
        cutoff = now - (self._window_ms * 2)  # Keep 2x window for safety

        # Find addresses to remove
        to_remove = []
        for addr, timestamps in self._requests.items():
            if not timestamps or ticks_diff(timestamps[-1], cutoff) < 0:
                to_remove.append(addr)

        # Remove them
        for addr in to_remove:
            del self._requests[addr]


class MiddlewareChain:
    """
    Chain multiple middleware components together.

    Processes requests through each middleware in order until one fails
    or all pass.
    """
    __slots__ = ('_middlewares',)

    def __init__(self):
        """Initialize empty middleware chain."""
        self._middlewares = []

    def add(self, middleware):
        """
        Add a middleware to the chain.

        Args:
            middleware: Middleware instance with check() method
        """
        self._middlewares.append(middleware)

    def process(self, connection, cmd, args):
        """
        Process request through all middlewares.

        Args:
            connection: ClientConnection instance
            cmd: bytes - Command name (uppercase)
            args: list - Command arguments

        Returns:
            bytes | None - Error response if any middleware fails, None if all pass
        """
        for mw in self._middlewares:
            # Auth middleware
            if isinstance(mw, AuthMiddleware):
                ok, response = mw.check_auth(connection, cmd, args)
                if not ok:
                    return response

            # Request validator
            elif isinstance(mw, RequestValidator):
                ok, response = mw.validate_request(cmd, args)
                if not ok:
                    return response

            # Rate limiter
            elif isinstance(mw, RateLimiter):
                if not mw.check_rate(connection.addr):
                    return ERR_RATE_LIMIT
                # Record the request (only if not rate limited)
                mw.record_request(connection.addr)

            # Generic middleware with check() method
            elif hasattr(mw, 'check'):
                ok, response = mw.check(connection, cmd, args)
                if not ok:
                    return response

        return None  # All middlewares passed

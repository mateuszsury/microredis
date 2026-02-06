"""
MicroRedis Exceptions Module

Defines the exception hierarchy for MicroRedis error handling.
Exceptions map to Redis error responses (RESP2 error strings).

Platform: ESP32-S3 with MicroPython
"""


class RedisError(Exception):
    """
    Base exception for all Redis errors.

    Attributes:
        prefix: str - Error prefix (e.g., 'ERR', 'WRONGTYPE')
    """

    prefix = 'ERR'

    def __init__(self, message=None):
        """
        Initialize Redis error.

        Args:
            message: str - Error message (without prefix)
        """
        self.message = message
        if message:
            super().__init__(f'{self.prefix} {message}')
        else:
            super().__init__(self.prefix)

    def to_resp(self):
        """
        Convert to RESP2 error string.

        Returns:
            bytes: RESP2-encoded error response
        """
        return f'-{str(self)}\r\n'.encode()


class WrongTypeError(RedisError):
    """
    Raised when a command is executed against a key of the wrong type.

    Example: GET on a hash key, LPUSH on a string key.
    """

    prefix = 'WRONGTYPE'

    def __init__(self):
        super().__init__('Operation against a key holding the wrong kind of value')


class RedisSyntaxError(RedisError):
    """
    Raised when a command has invalid syntax.
    Named RedisSyntaxError to avoid shadowing Python's builtin SyntaxError.
    """

    def __init__(self, message='syntax error'):
        super().__init__(message)


class OutOfMemoryError(RedisError):
    """
    Raised when memory limit is exceeded and command is not allowed.

    OOM = Out Of Memory
    """

    prefix = 'OOM'

    def __init__(self):
        super().__init__('command not allowed when used memory > maxmemory')


class AuthError(RedisError):
    """
    Raised when authentication fails.
    """

    prefix = 'WRONGPASS'

    def __init__(self, message='invalid username-password pair'):
        super().__init__(message)


class NoAuthError(RedisError):
    """
    Raised when authentication is required but not provided.
    """

    prefix = 'NOAUTH'

    def __init__(self):
        super().__init__('Authentication required')


class BusyError(RedisError):
    """
    Raised when server is busy with a long-running operation.
    """

    prefix = 'BUSY'

    def __init__(self, message='Server is busy'):
        super().__init__(message)


class ExecAbortError(RedisError):
    """
    Raised when EXEC aborts due to errors during MULTI.
    """

    prefix = 'EXECABORT'

    def __init__(self):
        super().__init__('Transaction discarded because of previous errors')


class NoScriptError(RedisError):
    """
    Raised when a script is not found (Lua scripting not supported).
    """

    prefix = 'NOSCRIPT'

    def __init__(self):
        super().__init__('No matching script. Please use EVAL.')


class ReadOnlyError(RedisError):
    """
    Raised when a write command is executed on a read-only replica.
    """

    prefix = 'READONLY'

    def __init__(self):
        super().__init__('You can\'t write against a read only replica')


class NotBusyError(RedisError):
    """
    Raised when SCRIPT KILL is called but no script is running.
    """

    prefix = 'NOTBUSY'

    def __init__(self):
        super().__init__('No scripts in execution right now')


class LoadingError(RedisError):
    """
    Raised when server is loading data from disk.
    """

    prefix = 'LOADING'

    def __init__(self):
        super().__init__('Redis is loading the dataset in memory')


class InvalidCursorError(RedisError):
    """
    Raised when an invalid cursor is used in SCAN family commands.
    """

    def __init__(self):
        super().__init__('invalid cursor')


class NotIntegerError(RedisError):
    """
    Raised when a value is not a valid integer.
    """

    def __init__(self):
        super().__init__('value is not an integer or out of range')


class NotFloatError(RedisError):
    """
    Raised when a value is not a valid float.
    """

    def __init__(self):
        super().__init__('value is not a valid float')


class IndexOutOfRangeError(RedisError):
    """
    Raised when an index is out of range.
    """

    def __init__(self):
        super().__init__('index out of range')


class NoSuchKeyError(RedisError):
    """
    Raised when a key doesn't exist but is required.
    """

    def __init__(self):
        super().__init__('no such key')


class StreamIdError(RedisError):
    """
    Raised when a stream ID is invalid or out of order.
    """

    def __init__(self, message='Invalid stream ID'):
        super().__init__(message)


class WrongArityError(RedisError):
    """
    Raised when a command has the wrong number of arguments.
    """

    def __init__(self, command_name):
        super().__init__(f"wrong number of arguments for '{command_name}' command")


class UnknownCommandError(RedisError):
    """
    Raised when an unknown command is received.
    """

    def __init__(self, command_name):
        super().__init__(f"unknown command '{command_name}'")


class ProtocolError(RedisError):
    """
    Raised when there's a protocol parsing error.
    """

    def __init__(self, message='Protocol error'):
        super().__init__(message)

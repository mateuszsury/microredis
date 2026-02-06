"""
MicroRedis Configuration Module

Provides configuration management for MicroRedis server with defaults
optimized for ESP32-S3 with MicroPython.

Platform: ESP32-S3 with MicroPython
RAM Constraints: ~300KB available for application
"""

try:
    from micropython import const
except ImportError:
    const = lambda x: x

from microredis.utils import glob_match as _match_pattern


# Default configuration values
DEFAULT_CONFIG = {
    # Network settings
    'bind': '0.0.0.0',
    'port': 6379,
    'maxclients': 8,
    'timeout': 300,  # Client idle timeout in seconds (0 = disabled)
    'tcp_keepalive': 300,

    # Memory settings
    'maxmemory': 0,  # 0 = no limit (use available RAM)
    'maxmemory_policy': 'allkeys-lru',  # Eviction policy when maxmemory reached
    'maxmemory_samples': 5,  # LRU sample size

    # Security
    'requirepass': None,  # Password for AUTH command (None = no auth)

    # Persistence
    'dbfilename': 'microredis.mrdb',
    'dir': '/data',
    'save': [],  # Save points: [(seconds, changes), ...]

    # Limits
    'hash_max_ziplist_entries': 64,
    'hash_max_ziplist_value': 512,
    'list_max_ziplist_size': -2,  # -2 = 8KB
    'set_max_intset_entries': 512,
    'zset_max_ziplist_entries': 128,
    'zset_max_ziplist_value': 64,

    # Server
    'hz': 10,  # Server Hz (background task frequency)
    'loglevel': 'notice',  # debug, verbose, notice, warning
    'logfile': '',  # Empty = stdout
    'databases': 1,  # Number of databases (only 1 supported)

    # Active expiry
    'active_expire_enabled': True,
    'active_expire_effort': 1,  # 1-10, effort level

    # Pub/Sub
    'client_output_buffer_limit_pubsub_hard': 64 * 1024,   # 64KB - ESP32 has ~300KB RAM
    'client_output_buffer_limit_pubsub_soft': 32 * 1024,   # 32KB
    'client_output_buffer_limit_pubsub_soft_seconds': 60,
}


class Config:
    """
    Configuration manager for MicroRedis.

    Provides get/set access to configuration values with validation.
    Uses __slots__ for memory efficiency.
    """

    __slots__ = ('_config',)

    def __init__(self, initial_config=None):
        """
        Initialize configuration with defaults.

        Args:
            initial_config: dict - Optional initial configuration to merge with defaults
        """
        self._config = dict(DEFAULT_CONFIG)
        if initial_config:
            for key, value in initial_config.items():
                if key in DEFAULT_CONFIG:
                    self._config[key] = value

    def get(self, key, default=None):
        """
        Get configuration value.

        Args:
            key: str - Configuration key
            default: Any - Default value if key not found

        Returns:
            Configuration value or default
        """
        return self._config.get(key, default)

    def set(self, key, value):
        """
        Set configuration value.

        Args:
            key: str - Configuration key
            value: Any - Value to set

        Returns:
            bool: True if key exists and was set, False if unknown key
        """
        if key in DEFAULT_CONFIG:
            self._config[key] = value
            return True
        return False

    def get_all(self):
        """
        Get all configuration values.

        Returns:
            dict: Copy of all configuration values
        """
        return dict(self._config)

    def get_matching(self, pattern):
        """
        Get configuration values matching glob pattern.

        Args:
            pattern: str - Glob pattern (supports * and ?)

        Returns:
            dict: Matching configuration key-value pairs
        """
        if pattern == '*':
            return self.get_all()

        result = {}
        for key, value in self._config.items():
            if _match_pattern(pattern, key):
                result[key] = value
        return result

    def rewrite(self):
        """
        Rewrite configuration to file (not implemented for ESP32).

        Returns:
            bool: Always False (not supported on ESP32)
        """
        return False

    def resetstat(self):
        """
        Reset statistics (currently a no-op).

        Returns:
            bool: True
        """
        return True


# _match_pattern is imported from utils.glob_match (canonical implementation)


# Global configuration instance
_global_config = None


def get_config():
    """
    Get global configuration instance.

    Returns:
        Config: Global configuration instance
    """
    global _global_config
    if _global_config is None:
        _global_config = Config()
    return _global_config


def init_config(config_dict=None):
    """
    Initialize global configuration.

    Args:
        config_dict: dict - Optional initial configuration

    Returns:
        Config: Initialized configuration instance
    """
    global _global_config
    _global_config = Config(config_dict)
    return _global_config

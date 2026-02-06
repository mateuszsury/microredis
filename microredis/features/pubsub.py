"""
Pub/Sub implementation for MicroRedis.

Provides publish/subscribe messaging with pattern matching support.
Memory-optimized for ESP32-S3 with ~300KB available RAM.
"""

try:
    import uasyncio as asyncio
except ImportError:
    import asyncio

from microredis.core.response import array, bulk_string, integer
from microredis.utils import glob_match as _glob_match


class PubSubManager:
    """
    Manages publish/subscribe channels and pattern subscriptions.

    Memory optimizations:
    - Uses sets for O(1) subscription checks
    - Reuses encoding helpers to avoid allocations
    - Tracks bidirectional mappings for fast cleanup
    """
    __slots__ = (
        '_channels',         # dict[bytes, set[connection]]
        '_patterns',         # dict[bytes, set[connection]]
        '_client_channels',  # dict[connection, set[bytes]]
        '_client_patterns',  # dict[connection, set[bytes]]
        '_message_count',    # int
    )

    def __init__(self):
        self._channels = {}
        self._patterns = {}
        self._client_channels = {}
        self._client_patterns = {}
        self._message_count = 0

    # ========== Subscribe Operations ==========

    def subscribe(self, connection, *channels):
        """
        Subscribe connection to one or more channels.

        Args:
            connection: ClientConnection instance
            channels: Channel names as bytes

        Returns:
            list[bytes]: RESP-encoded responses for each subscription
        """
        if connection not in self._client_channels:
            self._client_channels[connection] = set()

        responses = []
        client_subs = self._client_channels[connection]

        for channel in channels:
            # Add to channel subscribers
            if channel not in self._channels:
                self._channels[channel] = set()
            self._channels[channel].add(connection)

            # Add to client's subscriptions
            client_subs.add(channel)

            # Build response
            total_count = self.get_subscription_count(connection)
            responses.append(
                _encode_subscribe_response(b'subscribe', channel, total_count)
            )

        return responses

    def unsubscribe(self, connection, *channels):
        """
        Unsubscribe connection from channels.

        If no channels provided, unsubscribe from all channels.

        Args:
            connection: ClientConnection instance
            channels: Channel names as bytes (optional)

        Returns:
            list[bytes]: RESP-encoded responses for each unsubscription
        """
        client_subs = self._client_channels.get(connection, set())

        # If no channels specified, unsubscribe from all
        if not channels:
            channels = tuple(client_subs)

        responses = []

        for channel in channels:
            # Remove from channel subscribers
            if channel in self._channels:
                self._channels[channel].discard(connection)
                if not self._channels[channel]:
                    del self._channels[channel]  # Clean up empty sets

            # Remove from client's subscriptions
            client_subs.discard(channel)

            # Build response
            total_count = self.get_subscription_count(connection)
            responses.append(
                _encode_subscribe_response(b'unsubscribe', channel, total_count)
            )

        # Clean up empty client entry
        if not client_subs and connection in self._client_channels:
            del self._client_channels[connection]

        return responses

    def psubscribe(self, connection, *patterns):
        """
        Subscribe connection to one or more patterns.

        Args:
            connection: ClientConnection instance
            patterns: Pattern strings as bytes (glob-style)

        Returns:
            list[bytes]: RESP-encoded responses for each subscription
        """
        if connection not in self._client_patterns:
            self._client_patterns[connection] = set()

        responses = []
        client_patterns = self._client_patterns[connection]

        for pattern in patterns:
            # Add to pattern subscribers
            if pattern not in self._patterns:
                self._patterns[pattern] = set()
            self._patterns[pattern].add(connection)

            # Add to client's patterns
            client_patterns.add(pattern)

            # Build response
            total_count = self.get_subscription_count(connection)
            responses.append(
                _encode_subscribe_response(b'psubscribe', pattern, total_count)
            )

        return responses

    def punsubscribe(self, connection, *patterns):
        """
        Unsubscribe connection from patterns.

        If no patterns provided, unsubscribe from all patterns.

        Args:
            connection: ClientConnection instance
            patterns: Pattern strings as bytes (optional)

        Returns:
            list[bytes]: RESP-encoded responses for each unsubscription
        """
        client_patterns = self._client_patterns.get(connection, set())

        # If no patterns specified, unsubscribe from all
        if not patterns:
            patterns = tuple(client_patterns)

        responses = []

        for pattern in patterns:
            # Remove from pattern subscribers
            if pattern in self._patterns:
                self._patterns[pattern].discard(connection)
                if not self._patterns[pattern]:
                    del self._patterns[pattern]  # Clean up empty sets

            # Remove from client's patterns
            client_patterns.discard(pattern)

            # Build response
            total_count = self.get_subscription_count(connection)
            responses.append(
                _encode_subscribe_response(b'punsubscribe', pattern, total_count)
            )

        # Clean up empty client entry
        if not client_patterns and connection in self._client_patterns:
            del self._client_patterns[connection]

        return responses

    # ========== Publish Operations ==========

    async def publish(self, channel, message):
        """
        Publish message to channel.

        Delivers to:
        1. Direct channel subscribers
        2. Pattern subscribers whose patterns match the channel

        Args:
            channel: Channel name as bytes
            message: Message content as bytes

        Returns:
            int: Number of clients that received the message
        """
        recipients = set()

        # Direct channel subscribers
        if channel in self._channels:
            msg_encoded = _encode_message(channel, message)
            for conn in self._channels[channel]:
                try:
                    await conn.write_response(msg_encoded)
                    recipients.add(conn)
                except Exception:
                    # Connection failed - will be cleaned up on disconnect
                    pass

        # Pattern subscribers
        matched_patterns = self._match_patterns(channel)
        for pattern in matched_patterns:
            if pattern in self._patterns:
                msg_encoded = _encode_pmessage(pattern, channel, message)
                for conn in self._patterns[pattern]:
                    try:
                        await conn.write_response(msg_encoded)
                        recipients.add(conn)
                    except Exception:
                        # Connection failed
                        pass

        self._message_count += 1
        return len(recipients)

    def _match_patterns(self, channel):
        """
        Find all patterns that match the given channel.

        Args:
            channel: Channel name as bytes

        Returns:
            set[bytes]: Matching patterns
        """
        matches = set()
        for pattern in self._patterns:
            if _glob_match(pattern, channel):
                matches.add(pattern)
        return matches

    # ========== PUBSUB Subcommands ==========

    def pubsub_channels(self, pattern=None):
        """
        List active channels (with at least one subscriber).

        Args:
            pattern: Optional glob pattern as bytes

        Returns:
            list[bytes]: Channel names
        """
        if pattern is None:
            return list(self._channels.keys())

        # Filter by pattern
        return [ch for ch in self._channels if _glob_match(pattern, ch)]

    def pubsub_numsub(self, *channels):
        """
        Get subscriber count for each channel.

        Args:
            channels: Channel names as bytes

        Returns:
            list: Flat list of [channel1, count1, channel2, count2, ...]
        """
        result = []
        for channel in channels:
            count = len(self._channels.get(channel, set()))
            result.append(channel)
            result.append(count)
        return result

    def pubsub_numpat(self):
        """
        Get number of active pattern subscriptions.

        Returns:
            int: Total pattern subscriptions across all clients
        """
        # Count total pattern subscriptions (not unique patterns)
        total = 0
        for patterns in self._client_patterns.values():
            total += len(patterns)
        return total

    # ========== Client Management ==========

    def get_subscription_count(self, connection):
        """
        Get total subscription count for a client (channels + patterns).

        Args:
            connection: ClientConnection instance

        Returns:
            int: Total subscriptions
        """
        channel_count = len(self._client_channels.get(connection, set()))
        pattern_count = len(self._client_patterns.get(connection, set()))
        return channel_count + pattern_count

    def unsubscribe_all(self, connection):
        """
        Remove all subscriptions for a connection.

        Call this when a client disconnects to clean up resources.

        Args:
            connection: ClientConnection instance
        """
        # Unsubscribe from all channels
        if connection in self._client_channels:
            for channel in self._client_channels[connection]:
                if channel in self._channels:
                    self._channels[channel].discard(connection)
                    if not self._channels[channel]:
                        del self._channels[channel]
            del self._client_channels[connection]

        # Unsubscribe from all patterns
        if connection in self._client_patterns:
            for pattern in self._client_patterns[connection]:
                if pattern in self._patterns:
                    self._patterns[pattern].discard(connection)
                    if not self._patterns[pattern]:
                        del self._patterns[pattern]
            del self._client_patterns[connection]

    def is_subscribed(self, connection):
        """
        Check if connection has any active subscriptions.

        Args:
            connection: ClientConnection instance

        Returns:
            bool: True if client has subscriptions
        """
        return (connection in self._client_channels or
                connection in self._client_patterns)

    # ========== Statistics ==========

    def get_stats(self):
        """
        Get pub/sub statistics.

        Returns:
            dict: Statistics including channel/pattern counts
        """
        return {
            'channels': len(self._channels),
            'patterns': len(self._patterns),
            'subscribed_clients': len(self._client_channels) + len(self._client_patterns),
            'total_messages': self._message_count,
        }


# Pattern matching: uses glob_match from utils (imported at top)


# ========== Response Encoding Helpers ==========

def _encode_subscribe_response(msg_type, channel, count):
    """
    Encode subscribe/unsubscribe response.

    Format: [msg_type, channel, count]
    Example: ['subscribe', 'news', 1]

    Args:
        msg_type: 'subscribe', 'unsubscribe', 'psubscribe', 'punsubscribe' as bytes
        channel: Channel or pattern as bytes
        count: Total subscription count for client

    Returns:
        bytes: RESP-encoded array
    """
    return array([
        bulk_string(msg_type),
        bulk_string(channel),
        integer(count)
    ])


def _encode_message(channel, message):
    """
    Encode published message to channel subscriber.

    Format: ['message', channel, message]

    Args:
        channel: Channel name as bytes
        message: Message content as bytes

    Returns:
        bytes: RESP-encoded array
    """
    return array([
        bulk_string(b'message'),
        bulk_string(channel),
        bulk_string(message)
    ])


def _encode_pmessage(pattern, channel, message):
    """
    Encode published message to pattern subscriber.

    Format: ['pmessage', pattern, channel, message]

    Args:
        pattern: Matched pattern as bytes
        channel: Actual channel name as bytes
        message: Message content as bytes

    Returns:
        bytes: RESP-encoded array
    """
    return array([
        bulk_string(b'pmessage'),
        bulk_string(pattern),
        bulk_string(channel),
        bulk_string(message)
    ])

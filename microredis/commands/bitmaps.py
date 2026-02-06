"""
Bitmap operations for MicroRedis.

Provides Redis-compatible bitmap manipulation commands optimized for
MicroPython/ESP32-S3 with minimal memory overhead.

Target: ESP32-S3 (~300KB available RAM)
Author: MicroRedis Project
"""

try:
    from micropython import const
except ImportError:
    const = lambda x: x


class BitmapOperations:
    """
    Static bitmap operations for MicroRedis.

    All methods are static to avoid instance overhead.
    Uses big-endian bit ordering (Redis standard).
    """
    __slots__ = ()

    # BITOP operation types
    OP_AND = const(1)
    OP_OR = const(2)
    OP_XOR = const(3)
    OP_NOT = const(4)

    # BITFIELD overflow behaviors
    OVERFLOW_WRAP = const(0)
    OVERFLOW_SAT = const(1)
    OVERFLOW_FAIL = const(2)

    @staticmethod
    def setbit(storage, key, offset, value):
        """
        Set bit at offset to value (0 or 1).

        Args:
            storage: Storage engine instance
            key: Key as bytes
            offset: Bit offset (0-indexed)
            value: 0 or 1

        Returns:
            Previous bit value (0 or 1)

        Raises:
            ValueError: If offset < 0 or value not in {0, 1}
        """
        if offset < 0:
            raise ValueError("bit offset is not an integer or out of range")
        if value not in (0, 1):
            raise ValueError("bit is not an integer or out of range")

        # Redis uses big-endian bit ordering within bytes
        byte_index = offset >> 3  # offset // 8
        bit_index = 7 - (offset & 7)  # 7 - (offset % 8)

        # Get or create value
        data = bytearray(storage.get(key) or b'')

        # Extend if needed (pad with zero bytes)
        while len(data) <= byte_index:
            data.append(0)

        # Get old bit
        old_bit = (data[byte_index] >> bit_index) & 1

        # Set new bit
        if value:
            data[byte_index] |= (1 << bit_index)
        else:
            data[byte_index] &= ~(1 << bit_index)

        storage.set(key, bytes(data))
        return old_bit

    @staticmethod
    def getbit(storage, key, offset):
        """
        Get bit value at offset.

        Args:
            storage: Storage engine instance
            key: Key as bytes
            offset: Bit offset (0-indexed)

        Returns:
            Bit value (0 or 1)

        Raises:
            ValueError: If offset < 0
        """
        if offset < 0:
            raise ValueError("bit offset is not an integer or out of range")

        data = storage.get(key)
        if not data:
            return 0

        byte_index = offset >> 3  # offset // 8
        if byte_index >= len(data):
            return 0

        bit_index = 7 - (offset & 7)  # 7 - (offset % 8)
        return (data[byte_index] >> bit_index) & 1

    @staticmethod
    def bitcount(storage, key, start=None, end=None, mode=b'BYTE'):
        """
        Count set bits in string.

        Args:
            storage: Storage engine instance
            key: Key as bytes
            start: Start byte/bit index (optional, inclusive)
            end: End byte/bit index (optional, inclusive)
            mode: b'BYTE' or b'BIT' (default: BYTE)

        Returns:
            Number of set bits

        Note:
            Uses Brian Kernighan's algorithm for efficient bit counting.
            BIT mode is Redis 7.0+ feature.
        """
        data = storage.get(key)
        if not data:
            return 0

        # Apply range
        if mode == b'BIT':
            # Convert bit range to byte range
            if start is not None:
                bit_start = start if start >= 0 else (len(data) * 8 + start)
                byte_start = bit_start >> 3
                bit_start_offset = bit_start & 7
            else:
                byte_start = 0
                bit_start_offset = 0

            if end is not None:
                bit_end = end if end >= 0 else (len(data) * 8 + end)
                byte_end = bit_end >> 3
                bit_end_offset = bit_end & 7
            else:
                byte_end = len(data) - 1
                bit_end_offset = 7

            # Extract bit range and count
            count = 0
            for byte_idx in range(byte_start, min(byte_end + 1, len(data))):
                byte = data[byte_idx]

                # Mask first byte
                if byte_idx == byte_start and bit_start_offset > 0:
                    mask = (1 << (8 - bit_start_offset)) - 1
                    byte &= mask

                # Mask last byte
                if byte_idx == byte_end and bit_end_offset < 7:
                    mask = ~((1 << (7 - bit_end_offset)) - 1)
                    byte &= mask

                # Count bits in byte
                while byte:
                    byte &= byte - 1
                    count += 1

            return count
        else:
            # BYTE mode (default)
            if start is not None:
                # Normalize negative indices
                length = len(data)
                if start < 0:
                    start = max(0, length + start)
                if end is None:
                    end = length - 1
                elif end < 0:
                    end = length + end

                # Clamp to valid range
                start = max(0, min(start, length - 1))
                end = max(0, min(end, length - 1))

                if start > end:
                    return 0

                data = data[start:end+1]

            # Count bits using Brian Kernighan's algorithm
            count = 0
            for byte in data:
                while byte:
                    byte &= byte - 1
                    count += 1
            return count

    @staticmethod
    def bitpos(storage, key, bit, start=None, end=None, mode=b'BYTE'):
        """
        Find position of first bit set to 0 or 1.

        Args:
            storage: Storage engine instance
            key: Key as bytes
            bit: 0 or 1 to search for
            start: Start byte/bit index (optional)
            end: End byte/bit index (optional)
            mode: b'BYTE' or b'BIT'

        Returns:
            Bit position or -1 if not found

        Raises:
            ValueError: If bit not in {0, 1}
        """
        if bit not in (0, 1):
            raise ValueError("bit must be 0 or 1")

        data = storage.get(key)
        if not data:
            return -1 if bit == 1 else 0

        # Apply byte range (simplified - only BYTE mode for now)
        if mode == b'BYTE' and start is not None:
            length = len(data)
            if start < 0:
                start = max(0, length + start)
            if end is None:
                end = length - 1
            elif end < 0:
                end = length + end

            start = max(0, min(start, length - 1))
            end = max(0, min(end, length - 1))

            if start > end:
                return -1

            offset = start
            data = data[start:end+1]
        else:
            offset = 0

        # Search for bit
        for byte_idx, byte_val in enumerate(data):
            # Invert if searching for 0
            if bit == 0:
                byte_val = ~byte_val & 0xFF

            # Check if any bit is set
            if byte_val:
                # Find first set bit
                for bit_idx in range(8):
                    if byte_val & (0x80 >> bit_idx):
                        return (offset + byte_idx) * 8 + bit_idx

        return -1

    @staticmethod
    def bitop(storage, operation, destkey, *keys):
        """
        Perform bitwise operation between strings.

        Args:
            storage: Storage engine instance
            operation: b'AND', b'OR', b'XOR', or b'NOT'
            destkey: Destination key
            *keys: Source keys

        Returns:
            Length of result string

        Raises:
            ValueError: If invalid operation or wrong number of keys for NOT
        """
        # Parse operation
        if operation == b'AND':
            op = BitmapOperations.OP_AND
        elif operation == b'OR':
            op = BitmapOperations.OP_OR
        elif operation == b'XOR':
            op = BitmapOperations.OP_XOR
        elif operation == b'NOT':
            op = BitmapOperations.OP_NOT
        else:
            raise ValueError("unknown operation")

        # Validate NOT has exactly one source key
        if op == BitmapOperations.OP_NOT and len(keys) != 1:
            raise ValueError("BITOP NOT requires exactly one source key")

        if not keys:
            raise ValueError("at least one source key is required")

        # Get all source values
        values = [storage.get(key) or b'' for key in keys]

        # Find maximum length
        max_len = max(len(v) for v in values) if values else 0

        if max_len == 0:
            # All empty - store empty string
            storage.set(destkey, b'')
            return 0

        # Pad all values to same length
        padded = []
        for v in values:
            if len(v) < max_len:
                padded.append(bytearray(v) + bytearray(max_len - len(v)))
            else:
                padded.append(bytearray(v))

        # Perform operation
        result = bytearray(max_len)

        if op == BitmapOperations.OP_AND:
            # Start with all 1s
            for i in range(max_len):
                result[i] = 0xFF
            for v in padded:
                for i in range(max_len):
                    result[i] &= v[i]

        elif op == BitmapOperations.OP_OR:
            for v in padded:
                for i in range(max_len):
                    result[i] |= v[i]

        elif op == BitmapOperations.OP_XOR:
            for v in padded:
                for i in range(max_len):
                    result[i] ^= v[i]

        elif op == BitmapOperations.OP_NOT:
            for i in range(max_len):
                result[i] = (~padded[0][i]) & 0xFF

        # Store result
        storage.set(destkey, bytes(result))
        return max_len

    @staticmethod
    def bitfield(storage, key, *args):
        """
        Perform multiple bitfield operations in a single call.

        Args:
            storage: Storage engine instance
            key: Key as bytes
            *args: Command arguments (GET/SET/INCRBY type offset [value])

        Returns:
            List of results (int or None for each operation)

        Supported types: u1-u63, i1-i64 (unsigned/signed)
        Subcommands: GET, SET, INCRBY, OVERFLOW

        Example:
            BITFIELD mykey GET u4 0 SET u4 0 15 INCRBY u4 0 1
            -> [current_value, old_value, new_value]
        """
        if not args:
            return []

        # Get current data
        data = bytearray(storage.get(key) or b'')
        results = []
        overflow_mode = BitmapOperations.OVERFLOW_WRAP
        modified = False

        i = 0
        while i < len(args):
            cmd = args[i].upper()

            if cmd == b'OVERFLOW':
                # Set overflow mode for subsequent operations
                i += 1
                if i >= len(args):
                    raise ValueError("OVERFLOW requires a mode")
                mode = args[i].upper()
                if mode == b'WRAP':
                    overflow_mode = BitmapOperations.OVERFLOW_WRAP
                elif mode == b'SAT':
                    overflow_mode = BitmapOperations.OVERFLOW_SAT
                elif mode == b'FAIL':
                    overflow_mode = BitmapOperations.OVERFLOW_FAIL
                else:
                    raise ValueError("invalid overflow mode")
                i += 1
                continue

            # Parse type and offset
            if i + 2 >= len(args):
                raise ValueError("not enough arguments for {}".format(cmd))

            type_str = args[i + 1]
            offset = int(args[i + 2])

            signed, bits = BitmapOperations._parse_bitfield_type(type_str)

            if cmd == b'GET':
                value = BitmapOperations._get_bits(data, offset, bits, signed)
                results.append(value)
                i += 3

            elif cmd == b'SET':
                if i + 3 >= len(args):
                    raise ValueError("SET requires a value")
                new_value = int(args[i + 3])

                # Get old value
                old_value = BitmapOperations._get_bits(data, offset, bits, signed)

                # Set new value
                BitmapOperations._set_bits(data, offset, bits, new_value, signed, overflow_mode)
                modified = True
                results.append(old_value)
                i += 4

            elif cmd == b'INCRBY':
                if i + 3 >= len(args):
                    raise ValueError("INCRBY requires an increment")
                increment = int(args[i + 3])

                # Get current value
                current = BitmapOperations._get_bits(data, offset, bits, signed)

                # Calculate new value with overflow handling
                new_value = current + increment

                # Apply overflow mode
                if overflow_mode == BitmapOperations.OVERFLOW_WRAP:
                    # Wrap around
                    if signed:
                        max_val = (1 << (bits - 1)) - 1
                        min_val = -(1 << (bits - 1))
                        range_size = 1 << bits
                        while new_value > max_val:
                            new_value -= range_size
                        while new_value < min_val:
                            new_value += range_size
                    else:
                        max_val = (1 << bits) - 1
                        new_value &= max_val

                elif overflow_mode == BitmapOperations.OVERFLOW_SAT:
                    # Saturate at limits
                    if signed:
                        max_val = (1 << (bits - 1)) - 1
                        min_val = -(1 << (bits - 1))
                        new_value = max(min_val, min(max_val, new_value))
                    else:
                        max_val = (1 << bits) - 1
                        new_value = max(0, min(max_val, new_value))

                elif overflow_mode == BitmapOperations.OVERFLOW_FAIL:
                    # Check overflow
                    if signed:
                        max_val = (1 << (bits - 1)) - 1
                        min_val = -(1 << (bits - 1))
                        if new_value > max_val or new_value < min_val:
                            results.append(None)
                            i += 4
                            continue
                    else:
                        max_val = (1 << bits) - 1
                        if new_value > max_val or new_value < 0:
                            results.append(None)
                            i += 4
                            continue

                # Set new value
                BitmapOperations._set_bits(data, offset, bits, new_value, signed, overflow_mode)
                modified = True
                results.append(new_value)
                i += 4

            else:
                raise ValueError("unknown subcommand {}".format(cmd))

        # Save if modified
        if modified:
            storage.set(key, bytes(data))

        return results

    @staticmethod
    def _parse_bitfield_type(type_str):
        """
        Parse bitfield type specifier.

        Args:
            type_str: Type as bytes (e.g., b'u8', b'i16')

        Returns:
            Tuple of (signed: bool, bits: int)

        Raises:
            ValueError: If invalid type format
        """
        if not type_str or len(type_str) < 2:
            raise ValueError("invalid bitfield type")

        signed = type_str[0:1] == b'i'
        if not signed and type_str[0:1] != b'u':
            raise ValueError("invalid bitfield type")

        try:
            bits = int(type_str[1:])
        except ValueError:
            raise ValueError("invalid bitfield type")

        # Validate bit width
        if signed:
            if bits < 1 or bits > 64:
                raise ValueError("invalid bitfield type")
        else:
            if bits < 1 or bits > 63:
                raise ValueError("invalid bitfield type")

        return signed, bits

    @staticmethod
    def _get_bits(data, offset, bits, signed):
        """
        Extract bits from byte array.

        Args:
            data: Byte array
            offset: Bit offset
            bits: Number of bits
            signed: True for signed integer

        Returns:
            Integer value
        """
        # Calculate byte range
        byte_start = offset >> 3
        bit_start = offset & 7

        # Calculate how many bytes we need
        total_bits = bit_start + bits
        bytes_needed = (total_bits + 7) >> 3

        # Extract bytes (return 0 if out of range)
        value = 0
        for i in range(bytes_needed):
            byte_idx = byte_start + i
            if byte_idx < len(data):
                value = (value << 8) | data[byte_idx]
            else:
                value = value << 8

        # Shift to align
        shift = (bytes_needed * 8) - total_bits
        value >>= shift

        # Mask to bit width
        mask = (1 << bits) - 1
        value &= mask

        # Convert to signed if needed
        if signed and (value & (1 << (bits - 1))):
            value -= (1 << bits)

        return value

    @staticmethod
    def _set_bits(data, offset, bits, value, signed, overflow_mode):
        """
        Set bits in byte array.

        Args:
            data: Byte array (modified in place)
            offset: Bit offset
            bits: Number of bits
            value: Integer value to set
            signed: True for signed integer
            overflow_mode: Overflow behavior
        """
        # Clamp/wrap value to valid range
        if signed:
            max_val = (1 << (bits - 1)) - 1
            min_val = -(1 << (bits - 1))
            if overflow_mode == BitmapOperations.OVERFLOW_WRAP:
                range_size = 1 << bits
                while value > max_val:
                    value -= range_size
                while value < min_val:
                    value += range_size
            else:
                value = max(min_val, min(max_val, value))

            # Convert to unsigned representation
            if value < 0:
                value += (1 << bits)
        else:
            max_val = (1 << bits) - 1
            if overflow_mode == BitmapOperations.OVERFLOW_WRAP:
                value &= max_val
            else:
                value = max(0, min(max_val, value))

        # Calculate byte range
        byte_start = offset >> 3
        bit_start = offset & 7

        # Extend data if needed
        total_bits = bit_start + bits
        bytes_needed = (total_bits + 7) >> 3
        while len(data) < byte_start + bytes_needed:
            data.append(0)

        # Write bits
        remaining_bits = bits
        value_shift = bits

        for i in range(bytes_needed):
            byte_idx = byte_start + i

            if i == 0:
                # First byte - may be partial
                bits_in_byte = min(8 - bit_start, remaining_bits)
                shift = 8 - bit_start - bits_in_byte
                value_shift -= bits_in_byte

                # Create mask
                mask = ((1 << bits_in_byte) - 1) << shift

                # Extract value bits
                val_bits = ((value >> value_shift) & ((1 << bits_in_byte) - 1)) << shift

                # Update byte
                data[byte_idx] = (data[byte_idx] & ~mask) | val_bits

                remaining_bits -= bits_in_byte
            else:
                # Subsequent bytes
                bits_in_byte = min(8, remaining_bits)
                value_shift -= bits_in_byte

                if bits_in_byte == 8:
                    # Full byte
                    data[byte_idx] = (value >> value_shift) & 0xFF
                else:
                    # Partial byte (last byte)
                    shift = 8 - bits_in_byte
                    mask = ((1 << bits_in_byte) - 1) << shift
                    val_bits = ((value >> value_shift) & ((1 << bits_in_byte) - 1)) << shift
                    data[byte_idx] = (data[byte_idx] & ~mask) | val_bits

                remaining_bits -= bits_in_byte

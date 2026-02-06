"""
HyperLogLog probabilistic cardinality estimator for MicroRedis.

Implements HLL with 16384 registers (14-bit precision) for ~0.81% standard error.
Uses 12KB memory per HLL structure with 6-bit registers.

Memory: 16384 registers * 6 bits = 12288 bytes per HLL
"""

import math

# HyperLogLog constants
HLL_REGISTERS = 16384  # 2^14 registers
HLL_BITS = 6  # 6 bits per register (max value 63)
HLL_SIZE = 12288  # 16384 * 6 / 8 = 12288 bytes

# Bias correction constant for 16384 registers
# alpha_m = 0.7213 / (1 + 1.079 / m) for m >= 128
HLL_ALPHA = 0.7213 / (1 + 1.079 / HLL_REGISTERS)


class HyperLogLog:
    """
    HyperLogLog probabilistic cardinality estimator.

    All methods are static - no instance data required.
    Integrates directly with storage engine for persistence.
    """
    __slots__ = ()

    @staticmethod
    def _hash(element: bytes) -> int:
        """
        FNV-1a hash function - fast and simple for embedded systems.

        Args:
            element: Bytes to hash

        Returns:
            32-bit hash value
        """
        h = 2166136261  # FNV offset basis
        for byte in element:
            h ^= byte
            h = (h * 16777619) & 0xFFFFFFFF  # FNV prime, keep 32-bit
        return h

    @staticmethod
    def _count_leading_zeros(value: int, bits: int = 18) -> int:
        """
        Count leading zeros in the lower 'bits' bits of value.

        Args:
            value: Integer to count zeros in
            bits: Number of bits to consider (default 18 for 32-14=18 remaining)

        Returns:
            Count of leading zeros (0 to bits-1)
        """
        if value == 0:
            return bits

        count = 0
        # Check from highest bit downward
        mask = 1 << (bits - 1)
        while count < bits and not (value & mask):
            count += 1
            mask >>= 1
        return count

    @staticmethod
    def _get_register(data: bytes, index: int) -> int:
        """
        Get 6-bit register value from packed byte array.

        Registers are packed sequentially with 6 bits each.
        May span byte boundaries.

        Args:
            data: Packed register data
            index: Register index (0 to HLL_REGISTERS-1)

        Returns:
            Register value (0 to 63)
        """
        bit_offset = index * 6
        byte_offset = bit_offset >> 3  # Divide by 8
        bit_pos = bit_offset & 7  # Modulo 8

        # Read 2 bytes to handle cross-boundary registers
        val = data[byte_offset]
        if byte_offset + 1 < len(data):
            val |= data[byte_offset + 1] << 8

        # Extract 6 bits starting at bit_pos
        return (val >> bit_pos) & 0x3F

    @staticmethod
    def _set_register(data: bytearray, index: int, value: int) -> None:
        """
        Set 6-bit register value in packed byte array.

        Args:
            data: Packed register data (must be bytearray)
            index: Register index (0 to HLL_REGISTERS-1)
            value: Register value (0 to 63)
        """
        bit_offset = index * 6
        byte_offset = bit_offset >> 3
        bit_pos = bit_offset & 7

        # Clear the 6 bits at the target position
        mask = ~(0x3F << bit_pos)
        data[byte_offset] = (data[byte_offset] & (mask & 0xFF)) | ((value << bit_pos) & 0xFF)

        # Handle bits that spill into next byte (when bit_pos > 2)
        if bit_pos > 2 and byte_offset + 1 < len(data):
            data[byte_offset + 1] = (data[byte_offset + 1] & ((mask >> 8) & 0xFF)) | ((value << bit_pos) >> 8)

    @staticmethod
    def pfadd(storage, key: bytes, *elements: bytes) -> int:
        """
        Add elements to HyperLogLog.

        Creates new HLL if key doesn't exist.
        Uses hash to determine register and leading zero count.

        Args:
            storage: Storage engine instance
            key: HLL key
            elements: Elements to add

        Returns:
            1 if HLL was modified, 0 if not
        """
        if not elements:
            # Redis behavior: PFADD with no elements returns 1 if key doesn't exist
            existing = storage.get(key)
            if existing is None:
                storage.set(key, bytes(HLL_SIZE))
                return 1
            return 0

        # Get or create HLL structure
        existing = storage.get(key)
        data = bytearray(existing if existing else bytes(HLL_SIZE))

        modified = False

        for elem in elements:
            # Hash the element
            hash_val = HyperLogLog._hash(elem)

            # Use first 14 bits for register index (0 to 16383)
            register = hash_val & 0x3FFF

            # Count leading zeros in remaining 18 bits + 1
            remaining = hash_val >> 14
            count = HyperLogLog._count_leading_zeros(remaining, 18) + 1

            # Clamp to 6-bit range
            if count > 63:
                count = 63

            # Update register if new count is higher
            old_count = HyperLogLog._get_register(data, register)
            if count > old_count:
                HyperLogLog._set_register(data, register, count)
                modified = True

        # Save if modified
        if modified:
            storage.set(key, bytes(data))

        return 1 if modified else 0

    @staticmethod
    def pfcount(storage, *keys: bytes) -> int:
        """
        Estimate cardinality of HyperLogLog(s).

        If multiple keys provided, returns estimate of their union.
        Uses harmonic mean with bias correction.

        Args:
            storage: Storage engine instance
            keys: One or more HLL keys

        Returns:
            Estimated cardinality
        """
        if not keys:
            return 0

        # Merge all HLLs by taking max of each register
        merged = bytearray(HLL_SIZE)

        for key in keys:
            data = storage.get(key)
            if data:
                # Ensure data is correct size
                if len(data) != HLL_SIZE:
                    continue

                for i in range(HLL_REGISTERS):
                    val = HyperLogLog._get_register(data, i)
                    old = HyperLogLog._get_register(merged, i)
                    if val > old:
                        HyperLogLog._set_register(merged, i, val)

        # Calculate raw estimate using harmonic mean
        sum_inv = 0.0
        zeros = 0

        for i in range(HLL_REGISTERS):
            val = HyperLogLog._get_register(merged, i)
            sum_inv += 2.0 ** (-val)
            if val == 0:
                zeros += 1

        # Raw estimate: alpha * m^2 / sum(2^(-M[i]))
        estimate = HLL_ALPHA * HLL_REGISTERS * HLL_REGISTERS / sum_inv

        # Small range correction (estimate <= 2.5 * m)
        # Use linear counting for better accuracy
        if estimate <= 2.5 * HLL_REGISTERS and zeros > 0:
            estimate = HLL_REGISTERS * math.log(HLL_REGISTERS / zeros)

        # Large range correction not needed for 32-bit hash
        # (threshold would be 2^32 / 30 ~ 143M, far beyond our use case)

        return int(estimate)

    @staticmethod
    def pfmerge(storage, destkey: bytes, *sourcekeys: bytes) -> bool:
        """
        Merge multiple HyperLogLogs into destination.

        Takes maximum value of each register across all source HLLs.
        Creates new HLL at destkey if it doesn't exist.

        Args:
            storage: Storage engine instance
            destkey: Destination key
            sourcekeys: Source HLL keys to merge

        Returns:
            True (always succeeds in Redis)
        """
        # Start with empty HLL
        merged = bytearray(HLL_SIZE)

        # Include destination in merge if it exists
        dest_data = storage.get(destkey)
        if dest_data and len(dest_data) == HLL_SIZE:
            merged = bytearray(dest_data)

        # Merge all source HLLs
        for key in sourcekeys:
            data = storage.get(key)
            if data and len(data) == HLL_SIZE:
                for i in range(HLL_REGISTERS):
                    val = HyperLogLog._get_register(data, i)
                    old = HyperLogLog._get_register(merged, i)
                    if val > old:
                        HyperLogLog._set_register(merged, i, val)

        # Store merged result
        storage.set(destkey, bytes(merged))

        return True

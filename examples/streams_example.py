"""
Redis Streams Example for MicroRedis

Demonstrates how to use Redis Streams for time-series data logging
on ESP32-S3 with MicroPython.

Use Case: IoT Sensor Data Logging
- Log temperature and humidity readings from sensors
- Query historical data by time range
- Trim old data to manage memory

Platform: ESP32-S3 with MicroPython
"""

import sys
import time

# For testing on CPython
sys.path.insert(0, 'C:\\Users\\thete\\OneDrive\\Dokumenty\\PyCharm\\MicroRedis')

from microredis.storage.engine import Storage
from microredis.commands.streams import StreamOperations


def sensor_logging_example():
    """Example: Logging sensor data to a stream."""
    print("=" * 60)
    print("Example 1: Sensor Data Logging")
    print("=" * 60)

    storage = Storage()

    # Simulate sensor readings over time
    print("\nLogging sensor readings...")
    for i in range(10):
        temp = 20 + (i * 0.5)  # Simulated temperature
        humidity = 60 + (i * 2)  # Simulated humidity

        # Add entry with auto-generated timestamp ID
        entry_id = StreamOperations.xadd(
            storage,
            b'sensor:living_room',
            b'*',  # Auto-generate ID
            {
                b'temperature': f'{temp:.1f}'.encode(),
                b'humidity': str(humidity).encode(),
                b'unit_temp': b'C',
                b'unit_humidity': b'%'
            }
        )

        print(f"  [{entry_id.decode()}] temp={temp:.1f}C, humidity={humidity}%")
        time.sleep(0.01)  # 10ms between readings

    # Check stream length
    length = StreamOperations.xlen(storage, b'sensor:living_room')
    print(f"\nTotal readings logged: {length}")


def query_historical_data_example():
    """Example: Querying historical data from a stream."""
    print("\n" + "=" * 60)
    print("Example 2: Query Historical Data")
    print("=" * 60)

    storage = Storage()

    # Add some data
    ids = []
    for i in range(20):
        entry_id = StreamOperations.xadd(
            storage,
            b'sensor:outdoor',
            b'*',
            {
                b'temperature': f'{15 + i}'.encode(),
                b'pressure': f'{1013 + i}'.encode()
            }
        )
        ids.append(entry_id)
        time.sleep(0.01)

    print(f"\nLogged {len(ids)} readings")

    # Get all readings
    print("\n1. Get all readings (XRANGE - +):")
    all_entries = StreamOperations.xrange(storage, b'sensor:outdoor', b'-', b'+')
    print(f"   Retrieved {len(all_entries)} entries")

    # Get latest 5 readings
    print("\n2. Get latest 5 readings (XREVRANGE + - COUNT 5):")
    latest = StreamOperations.xrevrange(storage, b'sensor:outdoor', b'+', b'-', count=5)
    for entry_id, fields in latest:
        temp = fields[b'temperature'].decode()
        pressure = fields[b'pressure'].decode()
        print(f"   [{entry_id.decode()}] temp={temp}C, pressure={pressure}hPa")

    # Get range between two IDs
    print(f"\n3. Get readings between {ids[5].decode()} and {ids[10].decode()}:")
    range_entries = StreamOperations.xrange(storage, b'sensor:outdoor', ids[5], ids[10])
    print(f"   Retrieved {len(range_entries)} entries in range")


def multiple_streams_example():
    """Example: Reading from multiple streams."""
    print("\n" + "=" * 60)
    print("Example 3: Multiple Streams (XREAD)")
    print("=" * 60)

    storage = Storage()

    # Add data to multiple sensor streams
    print("\nLogging data to 3 different sensors...")

    # Sensor 1: Temperature
    for i in range(5):
        StreamOperations.xadd(
            storage,
            b'sensor:temp',
            b'*',
            {b'value': f'{20 + i}'.encode(), b'unit': b'C'}
        )
        time.sleep(0.01)

    # Sensor 2: Humidity
    for i in range(5):
        StreamOperations.xadd(
            storage,
            b'sensor:humidity',
            b'*',
            {b'value': f'{60 + i * 2}'.encode(), b'unit': b'%'}
        )
        time.sleep(0.01)

    # Sensor 3: Pressure
    for i in range(5):
        StreamOperations.xadd(
            storage,
            b'sensor:pressure',
            b'*',
            {b'value': f'{1013 + i}'.encode(), b'unit': b'hPa'}
        )
        time.sleep(0.01)

    # Read new data from all streams after ID 0-0
    print("\nReading from all sensors (XREAD):")
    result = StreamOperations.xread(
        storage,
        {
            b'sensor:temp': b'0-0',
            b'sensor:humidity': b'0-0',
            b'sensor:pressure': b'0-0'
        },
        count=3  # Get max 3 entries per stream
    )

    if result:
        for stream_key, entries in result:
            stream_name = stream_key.decode()
            print(f"\n  Stream: {stream_name}")
            for entry_id, fields in entries:
                value = fields[b'value'].decode()
                unit = fields[b'unit'].decode()
                print(f"    [{entry_id.decode()}] {value}{unit}")


def memory_management_example():
    """Example: Managing stream memory with XTRIM."""
    print("\n" + "=" * 60)
    print("Example 4: Memory Management (XTRIM)")
    print("=" * 60)

    storage = Storage()

    # Simulate continuous logging
    print("\nLogging 100 sensor readings...")
    for i in range(100):
        StreamOperations.xadd(
            storage,
            b'sensor:continuous',
            b'*',
            {
                b'reading': str(i).encode(),
                b'value': f'{20 + (i % 10)}'.encode()
            }
        )

    length = StreamOperations.xlen(storage, b'sensor:continuous')
    print(f"Stream length: {length} entries")

    # Trim to keep only last 20 entries (memory-efficient)
    print("\nTrimming stream to keep only last 20 entries...")
    deleted = StreamOperations.xtrim(storage, b'sensor:continuous', 20)
    print(f"Deleted {deleted} old entries")

    # Check new length
    length = StreamOperations.xlen(storage, b'sensor:continuous')
    print(f"New stream length: {length} entries")

    # Verify oldest entries were removed
    entries = StreamOperations.xrange(storage, b'sensor:continuous', b'-', b'+')
    first_reading = entries[0][1][b'reading'].decode()
    last_reading = entries[-1][1][b'reading'].decode()
    print(f"Remaining readings: {first_reading} to {last_reading}")


def real_world_iot_example():
    """Example: Real-world IoT scenario with multiple sensors and trimming."""
    print("\n" + "=" * 60)
    print("Example 5: Real-world IoT Scenario")
    print("=" * 60)

    storage = Storage()

    print("\nSimulating IoT device with 3 sensors over 30 readings...")
    print("(Each sensor logs every 100ms, auto-trim to keep last 10)")

    for i in range(30):
        # Temperature sensor
        temp_id = StreamOperations.xadd(
            storage,
            b'iot:temp',
            b'*',
            {
                b'celsius': f'{18 + (i % 10) * 0.5}'.encode(),
                b'fahrenheit': f'{64 + (i % 10) * 0.9}'.encode()
            }
        )

        # Auto-trim to keep memory under control
        if StreamOperations.xlen(storage, b'iot:temp') > 10:
            StreamOperations.xtrim(storage, b'iot:temp', 10)

        # Humidity sensor
        hum_id = StreamOperations.xadd(
            storage,
            b'iot:humidity',
            b'*',
            {b'percent': f'{55 + (i % 20)}'.encode()}
        )

        if StreamOperations.xlen(storage, b'iot:humidity') > 10:
            StreamOperations.xtrim(storage, b'iot:humidity', 10)

        # Light sensor
        light_id = StreamOperations.xadd(
            storage,
            b'iot:light',
            b'*',
            {b'lux': f'{200 + (i % 50) * 10}'.encode()}
        )

        if StreamOperations.xlen(storage, b'iot:light') > 10:
            StreamOperations.xtrim(storage, b'iot:light', 10)

        time.sleep(0.01)

    # Check final state
    print("\nFinal state after auto-trimming:")
    for stream_key in [b'iot:temp', b'iot:humidity', b'iot:light']:
        length = StreamOperations.xlen(storage, stream_key)
        print(f"  {stream_key.decode()}: {length} entries (max 10)")

    # Read latest data from all sensors
    print("\nLatest readings from all sensors:")
    result = StreamOperations.xread(
        storage,
        {
            b'iot:temp': b'0-0',
            b'iot:humidity': b'0-0',
            b'iot:light': b'0-0'
        },
        count=1  # Just the latest
    )

    if result:
        for stream_key, entries in result:
            if entries:
                entry_id, fields = entries[-1]  # Latest entry
                stream_name = stream_key.decode()
                field_str = ', '.join(f'{k.decode()}={v.decode()}'
                                     for k, v in fields.items())
                print(f"  {stream_name}: {field_str}")


def main():
    """Run all examples."""
    print("\n" + "=" * 60)
    print("MicroRedis Streams Examples")
    print("Redis Streams for IoT Sensor Data Logging")
    print("=" * 60)

    # Run all examples
    sensor_logging_example()
    query_historical_data_example()
    multiple_streams_example()
    memory_management_example()
    real_world_iot_example()

    print("\n" + "=" * 60)
    print("All examples completed successfully!")
    print("=" * 60)
    print("\nKey Takeaways:")
    print("- Use XADD with '*' for auto-generated timestamp IDs")
    print("- XRANGE/XREVRANGE for querying historical data")
    print("- XREAD for reading from multiple streams")
    print("- XTRIM to manage memory (critical on ESP32-S3)")
    print("- Keep stream size small on memory-constrained devices")
    print("=" * 60)


if __name__ == '__main__':
    main()

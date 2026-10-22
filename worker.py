
import json
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError

# =========================
# Kafka Configuration
# =========================
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "snmp"
GROUP_ID = "snmp-worker-group"


def main():
    print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}...")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        key_deserializer=lambda key: (
            key.decode("utf-8") if key else None
        ),
    )

    print(f"Subscribed to topic: {TOPIC}")
    print("Waiting for SNMP traffic messages... Press Ctrl+C to stop.\n")

    try:
        for message in consumer:
            data = message.value

            print("=" * 65)
            print(f"Received at: {datetime.now().isoformat(timespec='seconds')}")
            print(f"Kafka topic: {message.topic}")
            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")
            print(f"Router IP: {data.get('router_ip')}")
            print(f"Interface: {data.get('interface')}")
            print(f"Interface index: {data.get('if_index')}")
            print(f"Inbound counter: {data.get('in_octets')} bytes")
            print(f"Outbound counter: {data.get('out_octets')} bytes")
            print(f"Inbound traffic: {data.get('in_bps')} bps")
            print(f"Outbound traffic: {data.get('out_bps')} bps")

    except KeyboardInterrupt:
        print("\nWorker stopped.")

    except KafkaError as exc:
        print(f"Kafka error: {exc}")

    finally:
        consumer.close()
        print("Kafka consumer closed.")


if __name__ == "__main__":
    main()

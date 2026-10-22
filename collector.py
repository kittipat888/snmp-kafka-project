
import asyncio
import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from pysnmp.hlapi.v3arch.asyncio import (
    SnmpEngine,
    CommunityData,
    UdpTransportTarget,
    ContextData,
    ObjectType,
    ObjectIdentity,
    get_cmd,
)

# =========================
# Cisco Router Configuration
# =========================
ROUTER_IP = "192.168.149.136"
COMMUNITY = "public"
SNMP_PORT = 161

INTERFACE_NAME = "FastEthernet1/1"
IF_INDEX = 3

IN_OID = f"1.3.6.1.2.1.2.2.1.10.{IF_INDEX}"
OUT_OID = f"1.3.6.1.2.1.2.2.1.16.{IF_INDEX}"

# =========================
# Kafka Configuration
# =========================
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "snmp"

POLL_INTERVAL = 5
COUNTER_BITS = 32
COUNTER_MODULUS = 2 ** COUNTER_BITS


async def get_counter(engine, target, oid):
    result = await get_cmd(
        engine,
        CommunityData(COMMUNITY, mpModel=1),
        target,
        ContextData(),
        ObjectType(ObjectIdentity(oid)),
    )

    error_indication, error_status, error_index, var_binds = result

    if error_indication:
        raise RuntimeError(str(error_indication))

    if error_status:
        raise RuntimeError(error_status.prettyPrint())

    return int(var_binds[0][1])


async def main():
    print(f"Router: {ROUTER_IP}")
    print(f"Interface: {INTERFACE_NAME} (ifIndex {IF_INDEX})")
    print(f"Kafka broker: {BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {TOPIC}")

    # Connect to Kafka
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda data: json.dumps(data).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
        retries=5,
    )

    engine = SnmpEngine()

    try:
        target = await UdpTransportTarget.create(
            (ROUTER_IP, SNMP_PORT),
            timeout=2,
            retries=1,
        )

        print("Collector started. Press Ctrl+C to stop.\n")

        previous_in = None
        previous_out = None
        previous_time = None

        while True:
            start = time.monotonic()
            timestamp = datetime.now(timezone.utc).isoformat()

            try:
                in_bytes, out_bytes = await asyncio.gather(
                    get_counter(engine, target, IN_OID),
                    get_counter(engine, target, OUT_OID),
                )

                elapsed = (
                    start - previous_time
                    if previous_time is not None
                    else None
                )

                in_bps = None
                out_bps = None

                if (
                    previous_in is not None
                    and previous_out is not None
                    and elapsed is not None
                    and elapsed > 0
                ):
                    delta_in = (in_bytes - previous_in) % COUNTER_MODULUS
                    delta_out = (out_bytes - previous_out) % COUNTER_MODULUS

                    in_bps = round(delta_in * 8 / elapsed, 2)
                    out_bps = round(delta_out * 8 / elapsed, 2)

                message = {
                    "timestamp": timestamp,
                    "router_ip": ROUTER_IP,
                    "interface": INTERFACE_NAME,
                    "if_index": IF_INDEX,
                    "in_octets": in_bytes,
                    "out_octets": out_bytes,
                    "in_bps": in_bps,
                    "out_bps": out_bps,
                }

                # Publish JSON message to Kafka
                future = producer.send(
                    TOPIC,
                    key=INTERFACE_NAME,
                    value=message,
                )
                metadata = future.get(timeout=10)

                print(
                    f"[{timestamp}] "
                    f"IN={in_bytes:,} bytes | "
                    f"OUT={out_bytes:,} bytes | "
                    f"IN={in_bps} bps | "
                    f"OUT={out_bps} bps | "
                    f"Kafka={metadata.topic}[{metadata.partition}]"
                )

                previous_in = in_bytes
                previous_out = out_bytes
                previous_time = time.monotonic()

            except Exception as exc:
                print(f"[{timestamp}] Error: {exc}")

            elapsed_loop = time.monotonic() - start
            await asyncio.sleep(max(0, POLL_INTERVAL - elapsed_loop))

    finally:
        producer.close()
        engine.close()
        print("Collector stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCollector stopped.")


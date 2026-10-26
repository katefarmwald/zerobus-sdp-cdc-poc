"""
Zerobus producer for the SDP/DAB practice POC.

Generates an order change feed (INSERT / UPDATE / DELETE with a monotonic `seq`)
at a low, steady rate and pushes it to a Unity Catalog Delta table via Zerobus Ingest.
Orders move CREATED -> PAID -> SHIPPED, some get CANCELLED, a few get amount
corrections, and a small share are deleted. Built for practicing AUTO CDC (SCD 1 and 2).

Usage:
    python producer.py --count 5          # smoke test: send 5 events and exit
    python producer.py --rate 3           # run until Ctrl+C at ~3 events/sec
    python producer.py --rate 3 --minutes 15

Required environment variables (put them in a .env file that is NOT committed):
    ZEROBUS_SERVER_ENDPOINT   e.g. https://1234567890123456.zerobus.us-east-2.cloud.databricks.com
    DATABRICKS_WORKSPACE_URL  e.g. https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
    ZEROBUS_TABLE_NAME        e.g. workspace.zb_poc.orders_raw
    DATABRICKS_CLIENT_ID      service principal client (application) ID
    DATABRICKS_CLIENT_SECRET  service principal OAuth secret
"""

import argparse
import os
import random
import time
import uuid

from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import TableProperties, ZerobusException

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # fine if you export the variables yourself

STATUS_FLOW = {"CREATED": "PAID", "PAID": "SHIPPED"}
MAX_OPEN_ORDERS = 200  # keeps the in-memory state small


DELETE_PROBABILITY = 0.04  # share of "touch an existing order" events that are deletes

_last_seq = 0


def now_micros() -> int:
    # Zerobus expects TIMESTAMP values as epoch microseconds.
    return int(time.time() * 1_000_000)


def next_seq() -> int:
    """Strictly increasing sequence number for AUTO CDC's SEQUENCE BY.
    Based on the clock so it keeps increasing across producer restarts,
    and bumped by 1 if two events land in the same microsecond."""
    global _last_seq
    _last_seq = max(_last_seq + 1, now_micros())
    return _last_seq


def next_event(open_orders: dict) -> dict:
    """Emit a change-feed event: INSERT a new order, UPDATE an existing one
    (status change, occasional amount correction), or DELETE it."""
    if open_orders and random.random() < 0.6:
        order_id = random.choice(list(open_orders))
        current = open_orders[order_id]

        if random.random() < DELETE_PROBABILITY:
            operation = "DELETE"
            open_orders.pop(order_id)
            status = current["status"]
        else:
            operation = "UPDATE"
            if random.random() < 0.05:
                status = "CANCELLED"
            else:
                status = STATUS_FLOW.get(current["status"], "SHIPPED")
            if random.random() < 0.1:
                # amount correction, so SCD2 history has more than status changes
                current["amount"] = round(current["amount"] * random.uniform(0.9, 1.1), 2)
            if status in ("SHIPPED", "CANCELLED"):
                open_orders.pop(order_id)
            else:
                current["status"] = status
        base = current
    else:
        operation = "INSERT"
        order_id = str(uuid.uuid4())
        status = "CREATED"
        base = {
            # samples.tpch.customer keys, so gold can join to the sample data
            "customer_key": random.randint(1, 750_000),
            "amount": round(random.uniform(5, 500), 2),
        }
        if len(open_orders) < MAX_OPEN_ORDERS:
            open_orders[order_id] = {**base, "status": status}

    return {
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_key": base["customer_key"],
        "amount": base["amount"],
        "status": status,
        "operation": operation,
        "seq": next_seq(),
        "event_time": now_micros(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=3.0, help="events per second")
    parser.add_argument("--count", type=int, default=None, help="stop after N events")
    parser.add_argument("--minutes", type=float, default=None, help="stop after N minutes")
    args = parser.parse_args()

    sdk = ZerobusSdk(
        os.environ["ZEROBUS_SERVER_ENDPOINT"],
        os.environ["DATABRICKS_WORKSPACE_URL"],
    )
    stream = sdk.create_stream(
        os.environ["DATABRICKS_CLIENT_ID"],
        os.environ["DATABRICKS_CLIENT_SECRET"],
        TableProperties(os.environ["ZEROBUS_TABLE_NAME"]),
    )
    print("Stream open. Ctrl+C to stop.")

    open_orders: dict = {}
    sent = 0
    deadline = time.time() + args.minutes * 60 if args.minutes else None
    interval = 1.0 / args.rate

    try:
        while True:
            if args.count is not None and sent >= args.count:
                break
            if deadline and time.time() >= deadline:
                break
            stream.ingest_record_offset(next_event(open_orders))
            sent += 1
            if sent % 50 == 0 or args.count:
                print(f"sent {sent} events")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Stopping...")
    except ZerobusException as e:
        print(f"Zerobus error after {sent} events: {e}")
        raise
    finally:
        stream.close()  # flushes outstanding records
        print(f"Stream closed. Total sent: {sent}")


if __name__ == "__main__":
    main()
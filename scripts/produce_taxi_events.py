import json
import random
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

import typer
from confluent_kafka import Producer

app = typer.Typer(add_completion=False)


@dataclass(frozen=True)
class TaxiTripEvent:
    event_id: str
    event_ts: str  # ISO8601
    vendor_id: int
    pickup_ts: str
    dropoff_ts: str
    pu_location_id: int
    do_location_id: int
    passenger_count: int
    trip_distance: float
    fare_amount: float
    tip_amount: float
    total_amount: float
    payment_type: int  # 1=card, 2=cash, etc.
    ratecode_id: int


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def _make_event(now: datetime) -> TaxiTripEvent:
    pickup = now - timedelta(
        minutes=random.randint(5, 45), seconds=random.randint(0, 59)
    )
    dropoff = pickup + timedelta(
        minutes=random.randint(3, 40), seconds=random.randint(0, 59)
    )

    passenger_count = random.choice([1, 1, 1, 2, 2, 3, 4])
    trip_distance = round(max(0.1, random.gauss(2.5, 1.8)), 2)
    fare_amount = round(3.0 + trip_distance * random.uniform(1.8, 3.2), 2)
    tip_amount = round(fare_amount * random.choice([0.0, 0.0, 0.1, 0.15, 0.2]), 2)
    total_amount = round(fare_amount + tip_amount + random.uniform(0.0, 2.5), 2)

    return TaxiTripEvent(
        event_id=str(uuid.uuid4()),
        event_ts=_iso(now),
        vendor_id=random.choice([1, 2]),
        pickup_ts=_iso(pickup),
        dropoff_ts=_iso(dropoff),
        pu_location_id=random.randint(1, 265),
        do_location_id=random.randint(1, 265),
        passenger_count=passenger_count,
        trip_distance=trip_distance,
        fare_amount=fare_amount,
        tip_amount=tip_amount,
        total_amount=total_amount,
        payment_type=random.choice([1, 1, 1, 2, 2, 3]),
        ratecode_id=random.choice([1, 1, 1, 2, 3, 4, 5]),
    )


def _delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Delivery failed: {err}")
    # else: success; keep quiet to avoid noisy logs


@app.command()
def main(
    brokers: str = typer.Option(
        "localhost:9092", help="Kafka brokers, e.g. localhost:9092"
    ),
    topic: str = typer.Option("taxi_trips", help="Kafka topic name"),
    rate: int = typer.Option(10, help="Events per second"),
    minutes: int = typer.Option(1, help="How long to run"),
) -> None:
    """
    Produce synthetic NYC taxi-style events into Kafka.
    """
    producer = Producer(
        {
            "bootstrap.servers": brokers,
            "acks": "all",
            "retries": 5,
            "linger.ms": 20,
            # "enable.idempotence": True,
        }
    )

    end_time = time.time() + minutes * 60
    sleep_s = 1.0 / max(1, rate)

    typer.echo(
        f"Producing to topic='{topic}' brokers='{brokers}' rate={rate}/s for {minutes} min..."
    )

    sent = 0
    try:
        while time.time() < end_time:
            now = datetime.now(tz=timezone.utc)
            evt = _make_event(now)

            # Stable-ish key to exercise partitioning (ordering within partition)
            key = f"pu:{evt.pu_location_id}"

            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=json.dumps(asdict(evt)).encode("utf-8"),
                callback=_delivery_report,
            )

            # Let the producer serve delivery callbacks; non-blocking
            producer.poll(0)

            sent += 1
            if sent % (rate * 5) == 0:
                typer.echo(f"sent={sent}")

            time.sleep(sleep_s)

    finally:
        # Flush outstanding messages and delivery reports
        producer.flush(timeout=10)

    typer.echo(f"Done. Total events sent: {sent}")


if __name__ == "__main__":
    app()

"""Trimite eșantionul extras din DWH către Kafka, în loturi, cu pauze (simulare flux)."""
import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq
from confluent_kafka import Producer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.lab_common import KAFKA_BOOTSTRAP, KAFKA_TOPIC, STREAM_SAMPLE_DIR


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--batch-size", type=int, default=200)
    p.add_argument("--pause", type=float, default=2.0, help="secunde între loturi")
    p.add_argument("--limit", type=int, default=0, help="0 = tot eșantionul")
    p.add_argument("--skip", type=int, default=0, help="sare peste primele N evenimente")
    p.add_argument("--late-event-after", type=int, default=0,
                   help="după lotul N trimite un eveniment cu 10 zile mai vechi (test watermark)")
    args = p.parse_args()

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 20, "acks": "all"})
    files = sorted(STREAM_SAMPLE_DIR.glob("*.parquet"))
    if not files:
        sys.exit(f"Nu există eșantion în {STREAM_SAMPLE_DIR}. Rulați celula 4.1 din notebook.")

    sent, batch_no, last = 0, 0, None
    for f in files:
        for batch in pq.ParquetFile(f).iter_batches(batch_size=args.batch_size):
            rows = batch.to_pylist()
            if args.skip:
                drop = min(args.skip, len(rows)); rows = rows[drop:]; args.skip -= drop
            for r in rows:
                if args.limit and sent >= args.limit:
                    break
                r["ingest_time"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                producer.produce(KAFKA_TOPIC, key=r["business_id"], value=json.dumps(r))
                sent += 1; last = r
            producer.flush()
            batch_no += 1
            print(f"lot {batch_no}: total trimis {sent}, ultimul event_time={last and last['event_time']}")

            if args.late_event_after and batch_no == args.late_event_after and last:
                late = dict(last, review_id=f"LATE_{last['review_id']}", is_late_test=True)
                late["event_time"] = (datetime.strptime(last["event_time"], "%Y-%m-%d %H:%M:%S")
                                      - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")
                producer.produce(KAFKA_TOPIC, key=late["business_id"], value=json.dumps(late))
                producer.flush()
                print(f"  -> eveniment întârziat trimis: event_time={late['event_time']}")

            if args.limit and sent >= args.limit:
                return
            time.sleep(args.pause)


if __name__ == "__main__":
    main()

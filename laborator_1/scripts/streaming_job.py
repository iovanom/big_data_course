"""Kafka -> Spark Structured Streaming (ferestre pe event_time) -> MongoDB (upsert idempotent)."""
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F, types as T

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.lab_common import (CHECKPOINT_DIR, KAFKA_BOOTSTRAP, KAFKA_TOPIC,
                            MONGO_COLL, MONGO_DB, MONGO_URI, get_spark)

WINDOW = "1 day"
WATERMARK = "2 days"

EVENT_SCHEMA = T.StructType([
    T.StructField("review_id", T.StringType()), T.StructField("business_id", T.StringType()),
    T.StructField("city", T.StringType()), T.StructField("state", T.StringType()),
    T.StructField("main_category", T.StringType()), T.StructField("stars", T.DoubleType()),
    T.StructField("useful", T.LongType()), T.StructField("text_length", T.IntegerType()),
    T.StructField("event_time", T.StringType()), T.StructField("ingest_time", T.StringType()),
])


def build_stream(spark, starting_offsets="earliest"):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", KAFKA_TOPIC)
           .option("startingOffsets", starting_offsets)
           .option("maxOffsetsPerTrigger", 2000)
           .load())
    events = (raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
                         F.col("timestamp").alias("kafka_ts"))
              .select("e.*", "kafka_ts")
              .withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss")))
    agg = (events.withWatermark("event_time", WATERMARK)
           .groupBy(F.window("event_time", WINDOW))
           .agg(F.count("*").alias("n_reviews"),
                F.round(F.avg("stars"), 3).alias("avg_stars"),
                F.sum("useful").alias("sum_useful")))
    return events, agg


def write_to_mongo(batch_df, batch_id):
    """În modul update fiecare rând conține agregatul COMPLET al ferestrei -> $set e idempotent."""
    from pymongo import MongoClient, UpdateOne
    rows = batch_df.collect()
    if not rows:
        return
    now = datetime.now(timezone.utc)
    ops = [UpdateOne({"window_start": r["window"]["start"]},
                     {"$set": {"window_start": r["window"]["start"], "window_end": r["window"]["end"],
                               "n_reviews": r["n_reviews"], "avg_stars": r["avg_stars"],
                               "sum_useful": r["sum_useful"], "batch_id": batch_id, "updated_at": now}},
                     upsert=True) for r in rows]
    with MongoClient(MONGO_URI) as client:
        coll = client[MONGO_DB][MONGO_COLL]
        coll.create_index("window_start", unique=True)
        coll.bulk_write(ops, ordered=False)
    print(f"[batch {batch_id}] {len(ops)} ferestre scrise în MongoDB")


def start_query(spark, trigger="5 seconds", starting_offsets="earliest"):
    _, agg = build_stream(spark, starting_offsets)
    return (agg.writeStream.outputMode("update")
            .foreachBatch(write_to_mongo)
            .option("checkpointLocation", str(CHECKPOINT_DIR / "window_agg"))
            .trigger(processingTime=trigger)
            .queryName("yelp_window_agg")
            .start())


if __name__ == "__main__":
    spark = get_spark(app="yelp-streaming", driver_memory="2g", kafka=True)
    start_query(spark).awaitTermination()

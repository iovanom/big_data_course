#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
TOPIC="${KAFKA_TOPIC:-yelp_reviews}"

docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$TOPIC" || true
sleep 3
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic "$TOPIC" --partitions 3 --replication-factor 1
docker exec mongo mongosh -u "${MONGO_USER:-root}" -p "${MONGO_PASSWORD:-example}" \
  --authenticationDatabase admin --quiet --eval 'db.getSiblingDB("yelp_stream").window_agg.drop()'
rm -rf data/checkpoints/window_agg
echo "Reset complet: topic $TOPIC, colecția window_agg, checkpoint."

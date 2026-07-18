#!/bin/bash
echo "Waiting for Kafka to be ready..."
sleep 5

docker exec kafka kafka-topics --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "Topic 'orders' ready. Listing topics:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

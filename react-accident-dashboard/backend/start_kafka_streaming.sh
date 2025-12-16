#!/bin/bash

# Start Kafka/Redpanda Streaming System
# This script starts the producer and consumer for the accident dashboard

echo "=========================================="
echo "Starting Kafka/Redpanda Streaming System"
echo "=========================================="

# Check if Redpanda is running
echo "Checking Kafka/Redpanda connection..."
if ! nc -z localhost 9092 2>/dev/null; then
    echo "ERROR: Kafka/Redpanda is not running on localhost:9092"
    echo "Please start Redpanda first:"
    echo "  docker run -d -p 9092:9092 -p 9644:9644 --name redpanda docker.redpanda.com/vectorized/redpanda:latest redpanda start --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092 --pandaproxy-addr internal://0.0.0.0:8082,external://0.0.0.0:18082 --advertise-pandaproxy-addr internal://redpanda:8082,external://localhost:18082 --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:18081 --rpc-addr redpanda:33145 --advertise-rpc-addr redpanda:33145 --smp 1 --memory 1G --mode dev"
    echo ""
    echo "Or use docker-compose if you have a docker-compose.yml file"
    exit 1
fi

echo "✓ Kafka/Redpanda is running"

# Start producer in background
echo ""
echo "Starting producer..."
python kafka_producer_nyc.py --interval 5 &
PRODUCER_PID=$!

# Wait a bit for producer to start
sleep 2

# Start consumer (this will run in foreground)
echo "Starting consumer..."
echo "Press Ctrl+C to stop both producer and consumer"
echo ""

# Trap Ctrl+C to kill producer
trap "kill $PRODUCER_PID 2>/dev/null; exit" INT TERM

python kafka_consumer.py

# Cleanup
kill $PRODUCER_PID 2>/dev/null
echo ""
echo "Stopped all processes"


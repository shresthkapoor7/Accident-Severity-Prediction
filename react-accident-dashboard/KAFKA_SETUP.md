# Kafka/Redpanda Setup Guide

## Quick Start

### 1. Start Redpanda (Kafka-compatible)

```bash
docker run -d \
  -p 9092:9092 \
  -p 9644:9644 \
  --name redpanda \
  docker.redpanda.com/vectorized/redpanda:latest \
  redpanda start \
  --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 \
  --advertise-kafka-addr internal://redpanda:9092,external://localhost:19092 \
  --pandaproxy-addr internal://0.0.0.0:8082,external://0.0.0.0:18082 \
  --advertise-pandaproxy-addr internal://redpanda:8082,external://localhost:18082 \
  --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:18081 \
  --rpc-addr redpanda:33145 \
  --advertise-rpc-addr redpanda:33145 \
  --smp 1 \
  --memory 1G \
  --mode dev
```

Verify it's running:
```bash
docker ps | grep redpanda
```

### 2. Start the Producer (Terminal 1)

This sends mock NYC accident data to Kafka topics:

```bash
cd react-accident-dashboard/backend
python3 kafka_producer_nyc.py --interval 5
```

You should see:
```
✓ Connected to Kafka/Redpanda at localhost:9092
============================================================
NYC Accident Data Producer - Kafka/Redpanda
============================================================

Topic: accident-all (and accident-ny)
Interval: 5 seconds
============================================================

[1] ACC-NYC-... | Times Square - Broadway & W 47th St | 45.2°F | Clear
```

### 3. Start the Flask Backend (Terminal 2)

The backend includes the Kafka consumer:

```bash
cd react-accident-dashboard/backend
python3 app.py
```

### 4. Start the Frontend (Terminal 3)

```bash
cd react-accident-dashboard/frontend
npm start
```

### 5. Use the Dashboard

1. Navigate to http://localhost:3000
2. Go to the "Real-Time Prediction" tab
3. Click "Start Real-Time Stream"
4. You should see live predictions streaming in!

## How It Works

```
┌──────────────────┐
│  kafka_producer  │  Generates mock NYC accident data
│    _nyc.py       │  with real weather API calls
└────────┬─────────┘
         │
         │ Sends to Kafka topics:
         │ - accident-all
         │ - accident-ny
         │ - accident-weather-adverse (if adverse weather)
         │
         ▼
┌──────────────────┐
│  Redpanda/Kafka  │  Message broker
│  (Docker)        │
└────────┬─────────┘
         │
         │ Consumes messages
         │
         ▼
┌──────────────────┐
│  kafka_consumer  │  Consumes from Kafka
│      .py         │  Stores in memory queue
└────────┬─────────┘
         │
         │ Syncs to dashboard queue
         │
         ▼
┌──────────────────┐
│   app.py         │  Processes with ML model
│   (Flask API)    │  Streams via SSE to frontend
└────────┬─────────┘
         │
         │ Server-Sent Events
         │
         ▼
┌──────────────────┐
│  React Dashboard │  Displays live predictions
│  (Frontend)      │
└──────────────────┘
```

## Topics Created Automatically

Redpanda will automatically create topics when messages are sent:
- `accident-all` - All accident data
- `accident-ny` - New York state only
- `accident-weather-adverse` - Accidents with adverse weather

## API Endpoints

- `POST /api/streaming/start` - Start consuming from Kafka
- `POST /api/streaming/stop` - Stop consuming
- `GET /api/streaming/status` - Check status
- `POST /api/streaming/switch-state` - Switch to different state topic
- `GET /api/streaming/events` - SSE stream of predictions
- `GET /api/streaming/latest` - Get latest predictions

## Troubleshooting

### Redpanda not connecting?
```bash
# Check if Redpanda is running
docker ps | grep redpanda

# Check logs
docker logs redpanda

# Restart if needed
docker restart redpanda
```

### Producer can't connect?
- Make sure Redpanda is running on `localhost:9092`
- Check firewall settings
- Try: `nc -z localhost 9092` to test connection

### Consumer not receiving messages?
- Make sure producer is running and sending messages
- Check that you clicked "Start Real-Time Stream" in the dashboard
- Check backend logs for errors

### Topics not created?
Redpanda creates topics automatically when first message is sent. If you want to create manually:
```bash
docker exec -it redpanda rpk topic create accident-all --brokers localhost:9092
```

## Configuration

Edit these files to change settings:

- `kafka_producer_nyc.py`: Change `KAFKA_BOOTSTRAP_SERVERS` if Redpanda is on different host/port
- `kafka_consumer.py`: Change `KAFKA_BOOTSTRAP_SERVERS` to match producer
- `app.py`: Kafka consumer is imported from `kafka_consumer.py`

## Stop Everything

```bash
# Stop producer (Ctrl+C in Terminal 1)
# Stop backend (Ctrl+C in Terminal 2)
# Stop frontend (Ctrl+C in Terminal 3)
# Stop Redpanda
docker stop redpanda
docker rm redpanda
```


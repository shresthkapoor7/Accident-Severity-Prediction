# 🚗 US Accidents Analysis Dashboard (React + Flask + Spark + Kafka)

A full-stack React application for analyzing US traffic accidents with real-time streaming predictions using Kafka and Spark ML.

## Features

### 📊 Data Analysis Tab (Spark-powered)
- 🗺️ **Interactive US Map** - Hover over states to see accident statistics
- 📈 **Severity Analysis** - Bar and pie charts for severity distribution
- ⏰ **Time Analysis** - Accidents by hour and day of week
- 🌤️ **Weather Analysis** - Weather conditions and temperature statistics

### 🔮 Real-Time Prediction Tab (Kafka Streaming)
- **Live Streaming** - Real-time predictions for New York State
- **Mock Data Generator** - Simulates accident data streaming via Kafka
- **NY State Map** - Live markers showing prediction locations
- **Severity Statistics** - Real-time counters and distribution charts
- **Live Feed** - Scrolling feed of predictions with probability bars

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  Mock Data      │────▶│  Kafka       │────▶│  Spark ML       │
│  Generator      │     │  Topic       │     │  Model          │
└─────────────────┘     └──────────────┘     └────────┬────────┘
                                                       │
                                                       ▼
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  React UI       │◀────│  SSE         │◀────│  Flask API      │
│  Dashboard      │     │  Stream      │     │  Server         │
└─────────────────┘     └──────────────┘     └─────────────────┘
```

## Project Structure

```
react-accident-dashboard/
├── backend/
│   ├── app.py              # Flask API with SSE streaming
│   ├── kafka_producer.py   # Mock data generator for Kafka
│   ├── kafka_streaming.py  # Spark Streaming consumer
│   └── requirements.txt
└── frontend/
    ├── public/
    │   └── index.html
    └── src/
        ├── index.js
        ├── index.css
        ├── App.js          # Main React component
        └── App.css
```

## Prerequisites

- Python 3.8+
- Node.js 16+
- Apache Spark
- Kafka (optional, for full streaming setup)
- US Accidents dataset (`US_Accidents_March23.csv`)
- Trained model (`accident_severity_model/`)

## Installation

### Backend

```bash
cd backend
pip install -r requirements.txt
```

### Frontend

```bash
cd frontend
npm install
```

## Running the Application

### Option 1: Without Kafka (Mock Streaming)

The application includes a built-in mock streaming mode that doesn't require Kafka.

**Terminal 1 - Start Backend:**
```bash
cd react-accident-dashboard/backend
python3 app.py
```

**Terminal 2 - Start Frontend:**
```bash
cd react-accident-dashboard/frontend
npm start
```

Navigate to http://localhost:3000 and click "Start Real-Time Stream" in the Prediction tab.

### Option 2: With Kafka (Full Streaming)

**1. Start Zookeeper and Kafka:**
```bash
# macOS with Homebrew
brew services start zookeeper
brew services start kafka

# Or manually
zookeeper-server-start /opt/kafka/config/zookeeper.properties
kafka-server-start /opt/kafka/config/server.properties
```

**2. Create Kafka topic:**
```bash
kafka-topics --create --topic accident-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**3. Start the producer:**
```bash
cd backend
python3 kafka_producer.py --interval 3
```

**4. Start the streaming consumer:**
```bash
python3 kafka_streaming.py
```

## API Endpoints

### Data Analysis
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check |
| `/api/date-range` | GET | Available date range |
| `/api/summary` | GET | Summary statistics |
| `/api/state-stats` | GET | State-level statistics |
| `/api/time-analysis` | GET | Time-based analysis |
| `/api/weather-analysis` | GET | Weather analysis |

### Streaming Predictions
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/streaming/start` | POST | Start mock streaming |
| `/api/streaming/stop` | POST | Stop streaming |
| `/api/streaming/status` | GET | Get streaming status |
| `/api/streaming/events` | GET | SSE event stream |
| `/api/streaming/latest` | GET | Get recent predictions |

## Tech Stack

- **Frontend**: React 18, Recharts, react-simple-maps, SSE
- **Backend**: Flask, Flask-CORS
- **Data Processing**: Apache Spark (PySpark)
- **Streaming**: Kafka (optional), Server-Sent Events
- **ML Model**: Spark MLlib Random Forest

## Mock Data Generator

The mock data generator creates realistic accident data for New York State with:

- **Locations**: NYC boroughs, Buffalo, Syracuse, Rochester, Albany
- **Weather**: Weighted random selection (Clear most common)
- **Temperature**: Season-adjusted ranges
- **Time**: Uses current time for hour/day
- **Road Features**: Random crossing, junction, traffic signal flags

## Screenshots

The dashboard includes:
1. Two main tabs: Data Analysis and Real-Time Prediction
2. Interactive US map with hover tooltips
3. Live NY State map with prediction markers
4. Real-time severity counters and charts
5. Scrolling live prediction feed

# Accident Severity Prediction and Analysis System

A comprehensive big data analytics and machine learning system for predicting traffic accident severity in real-time, processing over 7.7 million accident records using Apache Spark and Kafka streaming.

## Overview

This project addresses the critical challenge of predicting traffic accident severity to enable emergency services to allocate resources more effectively. The system leverages distributed computing, advanced machine learning, and real-time streaming to provide both historical analytics and live predictions through an interactive web dashboard.

**Key Achievements:**
- Processes 7.7 million accident records without data sampling
- Handles severe class imbalance (79.67% Severity 2, <4% Severity 1 & 4)
- Sub-second prediction latency with Kafka streaming
- Interactive React dashboard with live predictions and analytics

## Problem Statement

Road traffic accidents in the United States occur at a massive scale, with over 6 million motor vehicle crashes annually. Traditional reporting is largely retrospective and static. This system bridges that gap by:
- Processing millions of historical records efficiently
- Training robust ML models to estimate accident severity
- Providing real-time insights into current risk conditions
- Exposing both batch analytics and live predictions through a web-based dashboard

## Features

### Historical Analytics
- **Summary Statistics**: Overview of accident distribution, severity levels, and key metrics
- **Geographic Analysis**: State-level risk rankings and accident density visualization on US map
- **Temporal Analysis**: Hourly, daily, weekly, and monthly accident patterns
- **Weather Analysis**: Impact of meteorological conditions on accident severity
- **Road Feature Analysis**: Correlation between infrastructure (junctions, crossings, signals) and severity

### Real-Time Capabilities
- **Live Predictions**: Streaming accident severity predictions with <1 second latency
- **Weather Integration**: Real-time weather data enrichment via OpenWeatherMap API
- **Interactive Map**: Live visualization of predicted accidents across the United States
- **State-Level Filtering**: Focus on specific geographic regions for targeted monitoring

### Machine Learning
- **One-vs-Rest Gradient Boosting Trees**: Optimized for multiclass imbalanced classification
- **Feature Engineering**: 15+ features including temporal, meteorological, and infrastructure attributes
- **Distributed Training**: Apache Spark MLlib for scalable model training
- **Class Weighting**: Handles severe class imbalance effectively

## Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────────────┐
│                    User Interface Layer                      │
│              (React Dashboard - Single Page App)             │
└────────────────────────┬────────────────────────────────────┘
                         │
            ┌────────────┴────────────┐
            │                         │
┌───────────▼──────────┐  ┌──────────▼───────────────────────┐
│  Backend Analytics   │  │   Streaming Infrastructure       │
│   & Inference API    │  │  (Kafka + Spark Streaming)       │
│   (Flask REST API)   │  │                                   │
└───────────┬──────────┘  └──────────┬───────────────────────┘
            │                         │
            │                         │
┌───────────▼─────────────────────────▼───────────────────────┐
│              Model Training Layer                            │
│         (Apache Spark MLlib - GBT Classifier)                │
└─────────────────────────┬────────────────────────────────────┘
                          │
                ┌─────────▼─────────┐
                │   Raw Dataset     │
                │  (7.7M records)   │
                └───────────────────┘
```

### Technology Stack

**Big Data Processing:**
- Apache Spark 3.x (PySpark)
- Spark MLlib for distributed ML
- Spark Structured Streaming

**Real-Time Streaming:**
- Apache Kafka
- Kafka Topics (state-level and aggregate)
- Server-Sent Events (SSE) for frontend

**Backend:**
- Python 3.x
- Flask REST API
- Kafka Producer/Consumer

**Frontend:**
- React.js
- Leaflet for maps
- Chart.js for visualizations

**External APIs:**
- OpenWeatherMap API for real-time weather data

## Dataset

**Source:** US Accidents (2016-2023) from Kaggle
- **Size:** 7.7 million records (3+ million for March 2023 alone)
- **Time Range:** 2016-2023
- **Geographic Coverage:** Countrywide (United States)
- **Features:** Location, time, weather, visibility, road infrastructure

**Key Attributes:**
- Severity (1-4 scale target variable)
- Geographic coordinates (latitude, longitude)
- Meteorological variables (temperature, humidity, pressure, visibility, wind speed)
- Temporal information (timestamp, hour, day of week)
- Road infrastructure (crossings, junctions, traffic signals)
- Weather conditions (clear, rain, snow, fog, etc.)

**Dataset Citation:**
1. Moosavi, Sobhan, et al. "A Countrywide Traffic Accident Dataset." arXiv preprint arXiv:1906.05409 (2019).
2. Moosavi, Sobhan, et al. "Accident Risk Prediction based on Heterogeneous Sparse Data." ACM SIGSPATIAL 2019.

## Getting Started

### Prerequisites

- Python 3.8+
- Apache Spark 3.x
- Apache Kafka (or Redpanda for lightweight alternative)
- Node.js 14+
- MongoDB (for data ingestion)
- 8GB+ RAM recommended

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/accident-collison-detection.git
   cd accident-collison-detection
   ```

2. **Set up MongoDB (for data ingestion):**
   ```bash
   cd db-ingestion
   chmod +x setup_mongodb.sh
   ./setup_mongodb.sh
   ```

3. **Install Python dependencies:**
   ```bash
   # For backend
   cd react-accident-dashboard/backend
   pip install -r requirements.txt

   # For database ingestion
   cd ../../db-ingestion
   pip install -r requirements.txt
   ```

4. **Install frontend dependencies:**
   ```bash
   cd react-accident-dashboard/frontend
   npm install
   ```

5. **Download the US Accidents dataset:**
   - Download from [Kaggle](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents)
   - Place the CSV file in the project root or appropriate data directory

### Configuration

1. **Set up OpenWeatherMap API:**
   - Get a free API key from [OpenWeatherMap](https://openweathermap.org/api)
   - Add to your environment variables or backend configuration

2. **Configure Spark:**
   - Adjust memory and parallelism settings in `accident_severity_prediction.py`
   - Default configuration optimized for 8GB RAM

3. **Set up Kafka:**
   - Follow instructions in `react-accident-dashboard/KAFKA_SETUP.md`
   - Topics are auto-created by the producer

## 📖 Usage

### 1. Train the Model

Run the model training script to process the dataset and train the GBT classifier:

```bash
spark-submit accident_severity_prediction.py
```

This will:
- Load and clean 7.7M records
- Engineer temporal and infrastructure features
- Train One-vs-Rest GBT classifier with class weighting
- Save the trained model to `accident_severity_model/`

Alternatively, use the Jupyter notebook for interactive exploration:
```bash
jupyter notebook modelTraining.ipynb
```

### 2. Load Data into MongoDB (Optional)

For additional analytics and data exploration:

```bash
cd db-ingestion
python loadInMongo.py
python analyze_collision_reasons.py
```

### 3. Start the Streaming Infrastructure

**Option A: Quick Start (All-in-One)**
```bash
cd react-accident-dashboard
chmod +x start.sh
./start.sh
```

**Option B: Manual Start**

Start Kafka:
```bash
# Start Zookeeper
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

# Start Kafka (in new terminal)
kafka-server-start /usr/local/etc/kafka/server.properties
```

Start Kafka producer (generates synthetic accidents):
```bash
cd react-accident-dashboard/backend
python kafka_producer_nyc.py
# or
python kafka_producer.py  # for nationwide coverage
```

Start Kafka streaming consumer:
```bash
cd react-accident-dashboard/backend
chmod +x start_kafka_streaming.sh
./start_kafka_streaming.sh
```

### 4. Launch the Backend API

```bash
cd react-accident-dashboard/backend
python app.py
```

The API will be available at `http://localhost:5000`

**Available Endpoints:**
- `GET /api/summary` - Overall statistics
- `GET /api/states` - State-level analysis
- `GET /api/time-analysis` - Temporal patterns
- `GET /api/weather-analysis` - Weather impact
- `GET /api/road-features` - Infrastructure analysis
- `POST /api/predict` - Single prediction
- `GET /api/stream/predictions` - Live predictions (SSE)
- `POST /api/stream/start` - Start streaming
- `POST /api/stream/stop` - Stop streaming

### 5. Launch the Dashboard

```bash
cd react-accident-dashboard/frontend
npm start
```

Access the dashboard at `http://localhost:3000`

**Dashboard Views:**
- **Overview**: Summary statistics and key charts
- **Geographic**: US map with accident density and state rankings
- **Temporal**: Hourly, daily, weekly, monthly patterns
- **Weather**: Environmental conditions and severity distribution
- **Live Predictions**: Real-time streaming predictions with interactive map

## Model Details

### Feature Engineering

**Temporal Features:**
- Hour of day (0-23)
- Day of week (0-6)
- Rush hour indicator (7-9 AM, 4-7 PM)
- Weekend indicator

**Meteorological Features:**
- Temperature (°F)
- Humidity (%)
- Pressure (inches)
- Visibility (miles)
- Wind speed (mph)
- Weather condition (categorical)

**Infrastructure Features:**
- Crossing presence (binary)
- Junction presence (binary)
- Traffic signal presence (binary)
- Stop sign presence (binary)

**Derived Features:**
- Temperature-humidity interaction
- Visibility-weather interaction

### Model Architecture

**Classifier:** One-vs-Rest Gradient Boosting Trees
- **Max Depth:** 10
- **Number of Trees:** 100
- **Max Iterations:** 20
- **Class Weighting:** Balanced to handle imbalance

**Training Strategy:**
1. StringIndexer for categorical features (weather, civil twilight)
2. OneHotEncoder for encoded categories
3. VectorAssembler for feature consolidation
4. One-vs-Rest wrapper around GBT classifier

### Performance Considerations

**Class Imbalance Handling:**
- Severity 2: 79.67% (majority class)
- Severity 3: 16.62%
- Severity 4: 2.09%
- Severity 1: 1.62%
- Solution: Class weighting + One-vs-Rest strategy

**Scalability:**
- Distributed processing via Spark
- No data sampling required
- Handles full 7.7M records
- Optimized for commodity hardware

## Real-Time Streaming Pipeline

### Architecture

1. **Producer** generates synthetic accidents
   - Draws from major US cities catalog
   - Enriches with live weather data (OpenWeatherMap)
   - Assigns road infrastructure attributes
   - Publishes to Kafka topics (per-state and aggregate)

2. **Consumer** processes events in micro-batches
   - Subscribes to Kafka topics
   - Reconstructs feature set
   - Applies trained ML model
   - Maintains rolling window of predictions

3. **Dashboard** receives updates via SSE
   - Displays live predictions on map
   - Shows severity distribution
   - Lists recent events with details

### Latency Performance

- End-to-end latency: <1 second
- Prediction latency: ~50-100ms
- Map update frequency: 2-5 seconds

## Project Structure

```
accident-collison-detection/
├── accident_severity_model/       # Trained Spark ML pipeline
│   ├── metadata/
│   └── stages/                    # Pipeline stage artifacts
├── accident_severity_prediction.py # Model training script
├── modelTraining.ipynb            # Interactive model training notebook
├── db-ingestion/                  # Data loading and analysis
│   ├── loadInMongo.py            # MongoDB ingestion
│   ├── analyze_collision_reasons.py
│   ├── setup_mongodb.sh
│   └── requirements.txt
├── react-accident-dashboard/      # Full-stack application
│   ├── backend/
│   │   ├── app.py                # Flask REST API
│   │   ├── kafka_consumer.py
│   │   ├── kafka_producer.py
│   │   ├── kafka_streaming.py    # Spark streaming consumer
│   │   └── requirements.txt
│   ├── frontend/
│   │   ├── src/
│   │   │   ├── App.js           # Main React application
│   │   │   ├── App.css
│   │   │   └── index.js
│   │   └── package.json
│   ├── KAFKA_SETUP.md
│   └── start.sh                  # All-in-one startup script
├── docs/                          # Documentation
│   ├── CONFERENCE_PAPER.md
│   └── TECHNICAL_REPORT.md
└── README.md
```

## Academic Context

**Course:** CS-GY 6513 Fall 2025 - Big Data
**Institution:** NYU Tandon School of Engineering
**Team Members:**
- Swapnil Sharma (ss19753)
- Uttam Singh (us2193)
- Shresth Kapoor (sk11677)

## Challenges & Learnings

### Key Challenges
1. **Limited Compute Resources:** Out-of-memory issues with 7.7M records on modest hardware
2. **Class Imbalance:** Severe skew toward Severity 2 (79.67%)
3. **Temporal Gap:** Training data ends in 2023; predictions for 2024+ are extrapolations
4. **Library Limitations:** Spark MLlib lacks some advanced calibration and explainability tools

### Solutions Implemented
- Careful memory tuning and Spark configuration
- One-vs-Rest + class weighting for imbalance
- Distributed processing to avoid data sampling
- Lightweight streaming alternatives (Redpanda compatible)

### Key Learnings
1. **Distributed Storage:** Multi-node solutions with partitioning would improve performance
2. **Lightweight Streaming:** Redpanda offers Kafka compatibility with lower resource footprint
3. **Advanced Streaming:** Apache Flink could enable more sophisticated real-time analytics
4. **Feature Engineering:** Domain knowledge critical for effective feature selection

## Future Work

1. **Enhanced Streaming:** Migrate to Apache Flink for event-time semantics and lower latency
2. **Advanced Models:** Experiment with XGBoost, LightGBM, or deep learning approaches
3. **Model Explainability:** Add SHAP values or LIME for prediction interpretability
4. **Alert System:** Automated notifications for high-severity predictions
5. **Mobile App:** Native mobile interface for emergency responders
6. **Historical Trends:** Incorporate multi-year trend analysis
7. **Integration:** Connect with real accident reporting systems
8. **Expanded Features:** Include traffic volume, road quality, driver demographics

## References

### Dataset
1. Moosavi, S., et al. (2019). "A Countrywide Traffic Accident Dataset." arXiv:1906.05409
2. Moosavi, S., et al. (2019). "Accident Risk Prediction based on Heterogeneous Sparse Data." ACM SIGSPATIAL

### Technologies
- [Apache Spark MLlib](https://spark.apache.org/docs/latest/ml-pipeline.html)
- [Apache Kafka](https://kafka.apache.org/)
- [OpenWeatherMap API](https://openweathermap.org/api)
- [Redpanda](https://redpanda.com/)

### Related Work
- Sun, H., Yang, H., Chen, J. (2024). "AI-based prediction of traffic crash severity." Heliyon
- Wang, Z., Zhang, J., Li, Q. (2024). "Traffic Accident Severity Level Prediction Model." Mobile Information Systems

## License

This project is developed for academic purposes as part of CS-GY 6513 Fall 2025.

## Acknowledgments

- **Dataset:** Sobhan Moosavi and collaborators for the US Accidents dataset
- **Tools:** Apache Software Foundation for Spark and Kafka
- **APIs:** OpenWeatherMap for weather data
- **AI Assistance:** LLMs were used for coding assistance and proofreading; all output was verified by authors

## Troubleshooting

### Common Issues

**Out of Memory Errors:**
- Reduce Spark executor memory in configuration
- Limit data loading with date filters
- Use data sampling for development/testing

**Kafka Connection Issues:**
- Ensure Zookeeper is running before Kafka
- Check port 9092 is not in use
- Verify topic creation with `kafka-topics --list`

**Model Loading Errors:**
- Ensure model is trained before starting backend
- Check Spark version compatibility
- Verify model path in backend configuration

**Frontend Not Connecting:**
- Confirm backend is running on port 5000
- Check CORS settings in Flask app
- Verify API endpoints are accessible

## Contact

For questions or collaboration:
- Swapnil Sharma: ss19753@nyu.edu
- Uttam Singh: us2193@nyu.edu
- Shresth Kapoor: sk11677@nyu.edu
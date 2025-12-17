"""
Flask Backend API for US Accidents Analysis Dashboard
Provides endpoints for data analysis and severity predictions using Spark
Includes SSE (Server-Sent Events) for real-time Kafka streaming predictions
"""

from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min as spark_min, max as spark_max
from pyspark.sql.types import TimestampType, StructType, StructField, DoubleType, StringType
from pyspark.ml import PipelineModel
from pyspark.ml.classification import GBTClassificationModel
from datetime import datetime
import os
import json
import time
import threading
import random
from collections import deque
import subprocess
import sys

from kafka import KafkaConsumer

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Real-time streaming predictions storage
streaming_predictions = deque(maxlen=50)
mock_streaming_active = False
mock_streaming_thread = None

# Background processes for Kafka-based streaming (spawned automatically)
producer_process = None
ml_streaming_process = None

# Configuration - paths relative to backend directory
# When running from backend folder, go up two levels to reach the project root
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
CSV_FILENAME = os.path.join(PROJECT_ROOT, "US_Accidents_March23.csv")
MODEL_PATH = os.path.join(PROJECT_ROOT, "accident_severity_model")
FEATURE_MODEL_PATH = MODEL_PATH + "_features"
OVR_MODEL_PATHS = {i: f"{MODEL_PATH}_severity_{i}" for i in range(4)}

# Kafka / Redpanda (Kafka API) configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
PREDICTION_TOPIC = "accident-predictions"

# Global variables
spark = None
df_spark = None
# Legacy single PipelineModel (RandomForest or similar)
severity_model = None
# One-vs-Rest GBT setup: feature pipeline + 4 binary models (severity 0–3)
feature_model = None
severity_models = {}
min_date = None
max_date = None

# Required columns for analysis
REQUIRED_COLUMNS = [
    'Start_Time', 'Severity', 'State', 'City', 'County',
    'Weather_Condition', 'Temperature(F)', 'Humidity(%)',
    'Pressure(in)', 'Visibility(mi)', 'Wind_Speed(mph)',
    'Crossing', 'Junction', 'Traffic_Signal', 'Sunrise_Sunset'
]

def init_spark():
    """Initialize Spark Session"""
    global spark, df_spark, min_date, max_date, severity_model, feature_model, severity_models

    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("US Accidents API") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("Spark initialized successfully!")

    # Load data
    print("Loading data reference...")
    df_spark = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(CSV_FILENAME) \
        .select(REQUIRED_COLUMNS)

    df_spark = df_spark.withColumn("Start_Time", col("Start_Time").cast(TimestampType()))

    # Get date range
    result = df_spark.select(
        spark_min(col("Start_Time").cast("date")).alias("min_date"),
        spark_max(col("Start_Time").cast("date")).alias("max_date")
    ).collect()[0]

    min_date = str(result["min_date"])
    max_date = str(result["max_date"])
    print(f"Date range: {min_date} to {max_date}")

    # Load prediction models
    # Prefer One-vs-Rest GBT models (severity 0–3) if available, otherwise fall back to legacy pipeline
    ovr_available = os.path.exists(FEATURE_MODEL_PATH) and all(os.path.exists(path) for path in OVR_MODEL_PATHS.values())

    if ovr_available:
        try:
            print(f"Loading feature pipeline from {FEATURE_MODEL_PATH}...")
            feature_model = PipelineModel.load(FEATURE_MODEL_PATH)

            for severity, path in OVR_MODEL_PATHS.items():
                print(f"Loading severity model {severity} from {path}...")
                severity_models[severity] = GBTClassificationModel.load(path)

            print("One-vs-Rest severity models (0–3) loaded successfully!")
        except Exception as e:
            print(f"Error loading One-vs-Rest models, will try legacy model instead: {e}")

    # Fallback: legacy single PipelineModel (older RandomForest-based model)
    if not severity_models:
        if os.path.exists(MODEL_PATH):
            try:
                print(f"Loading legacy prediction model from {MODEL_PATH}...")
                severity_model = PipelineModel.load(MODEL_PATH)
                print("Legacy prediction model loaded successfully!")
            except Exception as e:
                print(f"Error loading legacy model: {e}")
        else:
            print(f"No prediction models found at {MODEL_PATH} or OVR paths")

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'spark_initialized': spark is not None,
        'model_loaded': (feature_model is not None and bool(severity_models)) or severity_model is not None,
        'date_range': {'min': min_date, 'max': max_date}
    })

@app.route('/api/date-range', methods=['GET'])
def get_date_range():
    """Get available date range"""
    return jsonify({
        'min_date': min_date,
        'max_date': max_date
    })

@app.route('/api/summary', methods=['GET'])
def get_summary():
    """Get summary statistics for date range"""
    start_date = request.args.get('start_date', min_date)
    end_date = request.args.get('end_date', max_date)

    try:
        df_filtered = df_spark.filter(
            (col("Start_Time") >= start_date) &
            (col("Start_Time") <= f"{end_date} 23:59:59")
        )

        total_count = df_filtered.count()

        if total_count == 0:
            return jsonify({'error': 'No data found for the selected date range'}), 404

        # Get state count
        state_count = df_filtered.select("State").distinct().count()
        city_count = df_filtered.select("City").distinct().count()
        county_count = df_filtered.select("County").distinct().count()

        # Severity distribution
        severity_dist = df_filtered.groupBy("Severity").count().orderBy("Severity").collect()
        severity_data = {int(row['Severity']): row['count'] for row in severity_dist}

        return jsonify({
            'total_accidents': total_count,
            'states': state_count,
            'cities': city_count,
            'counties': county_count,
            'date_range': {'start': start_date, 'end': end_date},
            'severity_distribution': severity_data
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/state-stats', methods=['GET'])
def get_state_stats():
    """Get statistics by state for the US map"""
    start_date = request.args.get('start_date', min_date)
    end_date = request.args.get('end_date', max_date)

    try:
        df_filtered = df_spark.filter(
            (col("Start_Time") >= start_date) &
            (col("Start_Time") <= f"{end_date} 23:59:59")
        )

        # Aggregate by state
        state_stats = df_filtered.groupBy("State").agg(
            count("*").alias("total_accidents"),
            avg("Severity").alias("avg_severity"),
            avg("Temperature(F)").alias("avg_temperature"),
            avg("Visibility(mi)").alias("avg_visibility")
        ).collect()

        # Get severity breakdown per state
        severity_by_state = df_filtered.groupBy("State", "Severity").count().collect()
        severity_map = {}
        for row in severity_by_state:
            state = row['State']
            if state not in severity_map:
                severity_map[state] = {}
            severity_map[state][int(row['Severity'])] = row['count']

        # Get top city per state
        city_counts = df_filtered.groupBy("State", "City").count()
        # This is simplified - in production you'd want a proper window function

        result = []
        for row in state_stats:
            state = row['State']
            result.append({
                'state': state,
                'total_accidents': row['total_accidents'],
                'avg_severity': round(row['avg_severity'], 2) if row['avg_severity'] else 0,
                'avg_temperature': round(row['avg_temperature'], 1) if row['avg_temperature'] else 0,
                'avg_visibility': round(row['avg_visibility'], 1) if row['avg_visibility'] else 0,
                'severity_breakdown': severity_map.get(state, {})
            })

        return jsonify(result)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/time-analysis', methods=['GET'])
def get_time_analysis():
    """Get time-based analysis"""
    start_date = request.args.get('start_date', min_date)
    end_date = request.args.get('end_date', max_date)

    try:
        from pyspark.sql.functions import hour, dayofweek, month

        df_filtered = df_spark.filter(
            (col("Start_Time") >= start_date) &
            (col("Start_Time") <= f"{end_date} 23:59:59")
        )

        # Hour analysis
        hour_stats = df_filtered.withColumn("hour", hour("Start_Time")) \
            .groupBy("hour").count().orderBy("hour").collect()
        hour_data = {row['hour']: row['count'] for row in hour_stats}

        # Day of week analysis
        dow_stats = df_filtered.withColumn("dow", dayofweek("Start_Time")) \
            .groupBy("dow").count().orderBy("dow").collect()
        dow_data = {row['dow']: row['count'] for row in dow_stats}

        # Month analysis
        month_stats = df_filtered.withColumn("month", month("Start_Time")) \
            .groupBy("month").count().orderBy("month").collect()
        month_data = {row['month']: row['count'] for row in month_stats}

        return jsonify({
            'by_hour': hour_data,
            'by_day_of_week': dow_data,
            'by_month': month_data
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/weather-analysis', methods=['GET'])
def get_weather_analysis():
    """Get weather-based analysis"""
    start_date = request.args.get('start_date', min_date)
    end_date = request.args.get('end_date', max_date)

    try:
        df_filtered = df_spark.filter(
            (col("Start_Time") >= start_date) &
            (col("Start_Time") <= f"{end_date} 23:59:59")
        )

        # Weather condition counts (top 15 by volume)
        weather_stats = df_filtered.groupBy("Weather_Condition").count() \
            .orderBy(col("count").desc()).limit(15).collect()
        weather_data = {row['Weather_Condition']: row['count'] for row in weather_stats if row['Weather_Condition']}

        # Severity distribution by weather condition
        severity_stats = df_filtered.groupBy("Weather_Condition", "Severity").count().collect()
        severity_by_weather = {}
        for row in severity_stats:
            condition = row['Weather_Condition']
            if not condition:
                continue
            if condition not in severity_by_weather:
                severity_by_weather[condition] = {}
            severity_by_weather[condition][int(row['Severity'])] = row['count']

        # Temperature statistics
        temp_stats = df_filtered.select(
            avg("Temperature(F)").alias("avg"),
            spark_min("Temperature(F)").alias("min"),
            spark_max("Temperature(F)").alias("max")
        ).collect()[0]

        return jsonify({
            'weather_conditions': weather_data,
            'severity_by_weather': severity_by_weather,
            'temperature': {
                'avg': round(temp_stats['avg'], 1) if temp_stats['avg'] else 0,
                'min': round(temp_stats['min'], 1) if temp_stats['min'] else 0,
                'max': round(temp_stats['max'], 1) if temp_stats['max'] else 0
            }
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/road-analysis', methods=['GET'])
def get_road_analysis():
    """Get impact of key road features on accident volume and severity"""
    start_date = request.args.get('start_date', min_date)
    end_date = request.args.get('end_date', max_date)

    try:
        df_filtered = df_spark.filter(
            (col("Start_Time") >= start_date) &
            (col("Start_Time") <= f"{end_date} 23:59:59")
        )

        def build_feature_stats(feature_col):
            stats = df_filtered.groupBy(feature_col).agg(
                count("*").alias("total_accidents"),
                avg("Severity").alias("avg_severity")
            ).collect()
            return [
                {
                    "value": str(row[feature_col]),
                    "total_accidents": row["total_accidents"],
                    "avg_severity": round(row["avg_severity"], 2) if row["avg_severity"] is not None else 0,
                }
                for row in stats
                if row[feature_col] is not None
            ]

        return jsonify({
            "Crossing": build_feature_stats("Crossing"),
            "Junction": build_feature_stats("Junction"),
            "Traffic_Signal": build_feature_stats("Traffic_Signal"),
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============================================
# REAL-TIME WEATHER API INTEGRATION
# ============================================

import requests

# The Key is expired 😏
OPENWEATHER_API_KEY = "c5757baa8f23c85bedf0902235044704"
OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"

# NYC Locations with specific coordinates and street info
NYC_LOCATIONS = [
    (40.7580, -73.9855, "Times Square", "Broadway & W 47th St"),
    (40.7484, -73.9857, "Empire State Building", "5th Ave & W 34th St"),
    (40.7527, -73.9772, "Grand Central", "E 42nd St & Park Ave"),
    (40.7614, -73.9776, "Rockefeller Center", "W 50th St & 6th Ave"),
    (40.7794, -73.9632, "Upper East Side", "E 79th St & Madison Ave"),
    (40.7831, -73.9712, "Upper West Side", "W 79th St & Broadway"),
    (40.7282, -73.7949, "Queens - Jamaica", "Jamaica Ave & 169th St"),
    (40.7448, -73.9485, "Long Island City", "Jackson Ave & 44th Dr"),
    (40.6892, -74.0445, "Brooklyn - Downtown", "Fulton St & Adams St"),
    (40.6782, -73.9442, "Brooklyn - Prospect Park", "Flatbush Ave & Eastern Pkwy"),
    (40.8176, -73.9419, "Harlem", "W 125th St & Malcolm X Blvd"),
    (40.8448, -73.8648, "Bronx - Fordham", "Fordham Rd & Grand Concourse"),
    (40.5795, -74.1502, "Staten Island Ferry", "Richmond Terrace"),
    (40.7061, -74.0087, "Financial District", "Wall St & Broadway"),
    (40.7411, -73.9897, "Chelsea", "W 23rd St & 6th Ave"),
    (40.7295, -73.9965, "Greenwich Village", "Bleecker St & 6th Ave"),
    (40.7336, -74.0027, "SoHo", "Broadway & Spring St"),
    (40.7169, -73.9983, "Chinatown", "Canal St & Centre St"),
    (40.7272, -74.0048, "Tribeca", "Greenwich St & Chambers St"),
]

# Weather cache to avoid hitting API too frequently
weather_cache = {}
cache_timestamp = {}
CACHE_DURATION = 300  # 5 minutes

def get_real_weather(lat, lng, city):
    """Fetch real weather data from OpenWeatherMap API"""
    cache_key = f"{lat:.2f},{lng:.2f}"
    now = time.time()

    # Check cache
    if cache_key in weather_cache and (now - cache_timestamp.get(cache_key, 0)) < CACHE_DURATION:
        return weather_cache[cache_key]

    try:
        params = {
            "lat": lat,
            "lon": lng,
            "appid": OPENWEATHER_API_KEY,
            "units": "imperial"  # Fahrenheit
        }
        response = requests.get(OPENWEATHER_URL, params=params, timeout=5)

        if response.status_code == 200:
            data = response.json()

            # Extract weather data
            weather_data = {
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"] * 0.02953,  # Convert hPa to inHg
                "visibility": data.get("visibility", 10000) / 1609.34,  # Convert m to miles
                "wind_speed": data["wind"]["speed"],
                "weather_condition": data["weather"][0]["main"],
                "weather_description": data["weather"][0]["description"],
                "clouds": data.get("clouds", {}).get("all", 0),
            }

            # Map OpenWeather conditions to our model's expected values
            condition_map = {
                "Clear": "Clear",
                "Clouds": "Cloudy",
                "Rain": "Rain",
                "Drizzle": "Light Rain",
                "Thunderstorm": "Thunderstorm",
                "Snow": "Snow",
                "Mist": "Fog",
                "Fog": "Fog",
                "Haze": "Haze",
                "Smoke": "Smoke",
                "Dust": "Dust",
            }
            weather_data["weather_condition"] = condition_map.get(
                weather_data["weather_condition"],
                weather_data["weather_condition"]
            )

            # Refine based on description
            desc = weather_data["weather_description"].lower()
            if "heavy" in desc:
                if "rain" in desc:
                    weather_data["weather_condition"] = "Heavy Rain"
                elif "snow" in desc:
                    weather_data["weather_condition"] = "Heavy Snow"
            elif "light" in desc:
                if "rain" in desc:
                    weather_data["weather_condition"] = "Light Rain"
                elif "snow" in desc:
                    weather_data["weather_condition"] = "Light Snow"
            elif "overcast" in desc:
                weather_data["weather_condition"] = "Overcast"
            elif "partly" in desc or "scattered" in desc:
                weather_data["weather_condition"] = "Partly Cloudy"

            # Cache the result
            weather_cache[cache_key] = weather_data
            cache_timestamp[cache_key] = now

            print(f"Live weather for {city}: {weather_data['temperature']:.1f}°F, {weather_data['weather_condition']}")
            return weather_data

    except Exception as e:
        print(f"Weather API error for {city}: {e}")

    # Fallback to cached or default data
    if cache_key in weather_cache:
        return weather_cache[cache_key]

    # Default fallback
    return {
        "temperature": 50.0,
        "humidity": 50.0,
        "pressure": 29.92,
        "visibility": 10.0,
        "wind_speed": 5.0,
        "weather_condition": "Clear",
    }

def generate_mock_ny_accident():
    """Generate mock accident data for NYC using REAL weather data"""
    location = random.choice(NYC_LOCATIONS)
    lat, lng, neighborhood, street = location

    # Add small randomness to exact location (within ~0.005 degrees = ~500m)
    lat += random.uniform(-0.005, 0.005)
    lng += random.uniform(-0.005, 0.005)

    now = datetime.now()
    hour = now.hour
    day_of_week = 1 if now.isoweekday() == 7 else now.isoweekday() + 1

    # Get REAL weather data from API
    weather = get_real_weather(lat, lng, neighborhood)

    # Determine day/night based on sunrise/sunset (simplified)
    sunrise_sunset = "Day" if 6 <= hour <= 18 else "Night"

    return {
        "id": f"ACC-NYC-{now.strftime('%H%M%S')}-{random.randint(100, 999)}",
        "timestamp": now.isoformat(),
        "latitude": round(lat, 6),
        "longitude": round(lng, 6),
        "neighborhood": neighborhood,
        "street": street,
        "city": "New York City",
        "state": "NY",
        "hour": hour,
        "day_of_week": day_of_week,
        "temperature": round(weather["temperature"], 1),
        "humidity": round(weather["humidity"], 1),
        "pressure": round(weather["pressure"], 2),
        "visibility": round(min(weather["visibility"], 10), 1),  # Cap at 10 miles
        "wind_speed": round(weather["wind_speed"], 1),
        "weather_condition": weather["weather_condition"],
        "sunrise_sunset": sunrise_sunset,
        "crossing": random.random() < 0.3,
        "junction": random.random() < 0.4,
        "traffic_signal": random.random() < 0.6,
        "weather_source": "OpenWeatherMap Live"
    }

def predict_severity_with_ovr(input_df):
    """Run the One-vs-Rest GBT severity models on a single-row input_df.

    Returns (severity_index_0_3, probabilities_list_length_4).
    """
    global feature_model, severity_models

    if feature_model is None or not severity_models:
        raise RuntimeError("One-vs-Rest severity models are not loaded")

    # Prepare features using the feature pipeline (adds 'features' column)
    features_df = feature_model.transform(input_df)

    # Score with each binary GBT model (severity 0–3) and collect P(label=1) for each
    probs = []
    for severity_idx in sorted(severity_models.keys()):
        model = severity_models[severity_idx]
        pred_df = model.transform(features_df)
        row = pred_df.select("probability").first()
        prob_vec = row["probability"]
        # probability vector is [P(class=0), P(class=1)]
        probs.append(float(prob_vec[1]))

    # Normalize to make them easier to interpret as class probabilities
    total = sum(probs)
    if total > 0:
        probs_norm = [p / total for p in probs]
    else:
        probs_norm = [0.25] * len(probs)

    # Choose severity index with highest probability (0–3)
    best_idx = max(range(len(probs_norm)), key=lambda i: probs_norm[i])
    return best_idx, probs_norm


def make_prediction_for_streaming(data):
    """Make prediction for streaming data using the model"""
    global severity_model, feature_model, severity_models, spark

    if (feature_model is None or not severity_models) and severity_model is None:
        # Return mock prediction if no model is loaded
        return {
            "predicted_severity": random.choices([2, 2, 2, 3, 3, 4], weights=[30, 30, 20, 10, 5, 5])[0],
            "probabilities": [0.1, 0.5, 0.3, 0.1]
        }

    try:
        hour_val = data['hour']
        day_of_week = data['day_of_week']
        # Cast numeric fields explicitly to float to satisfy Spark DoubleType schema
        temperature = float(data['temperature'])
        humidity = float(data['humidity'])
        pressure = float(data['pressure'])
        visibility = float(data['visibility'])
        wind_speed = float(data['wind_speed'])
        weather_condition = data['weather_condition']
        sunrise_sunset = data['sunrise_sunset']
        crossing = 1.0 if data['crossing'] else 0.0
        junction = 1.0 if data['junction'] else 0.0
        traffic_signal = 1.0 if data['traffic_signal'] else 0.0

        is_rush_hour = 1.0 if (7 <= hour_val <= 9) or (16 <= hour_val <= 19) else 0.0
        is_weekend = 1.0 if day_of_week in [1, 7] else 0.0
        time_of_day = 0.0 if sunrise_sunset == "Day" else 1.0
        month_val = float(datetime.now().month)

        if month_val in [12, 1, 2]:
            season = 0.0
        elif month_val in [3, 4, 5]:
            season = 1.0
        elif month_val in [6, 7, 8]:
            season = 2.0
        else:
            season = 3.0

        input_data = [(
            data['latitude'], data['longitude'], float(hour_val), float(day_of_week),
            is_rush_hour, is_weekend, time_of_day, month_val, season,
            temperature, humidity, pressure, visibility, wind_speed,
            crossing, junction, traffic_signal,
            temperature * humidity, wind_speed * visibility,
            weather_condition, sunrise_sunset, 1.0
        )]

        schema = StructType([
            StructField("Start_Lat", DoubleType(), True),
            StructField("Start_Lng", DoubleType(), True),
            StructField("Hour", DoubleType(), True),
            StructField("DayOfWeek", DoubleType(), True),
            StructField("IsRushHour", DoubleType(), True),
            StructField("IsWeekend", DoubleType(), True),
            StructField("TimeOfDay", DoubleType(), True),
            StructField("Month", DoubleType(), True),
            StructField("Season", DoubleType(), True),
            StructField("Temperature(F)", DoubleType(), True),
            StructField("Humidity(%)", DoubleType(), True),
            StructField("Pressure(in)", DoubleType(), True),
            StructField("Visibility(mi)", DoubleType(), True),
            StructField("Wind_Speed(mph)", DoubleType(), True),
            StructField("Crossing", DoubleType(), True),
            StructField("Junction", DoubleType(), True),
            StructField("Traffic_Signal", DoubleType(), True),
            StructField("Temp_Humidity_Interaction", DoubleType(), True),
            StructField("Wind_Visibility_Interaction", DoubleType(), True),
            StructField("Weather_Condition", StringType(), True),
            StructField("Sunrise_Sunset", StringType(), True),
            StructField("classWeight", DoubleType(), True)
        ])

        input_df = spark.createDataFrame(input_data, schema)

        # Prefer One-vs-Rest models if available, otherwise fall back to legacy model
        if feature_model is not None and severity_models:
            severity_idx, probs = predict_severity_with_ovr(input_df)
            # Convert 0–3 index back to severity 1–4 for external consumers
            predicted_severity = severity_idx + 1
            return {
                "predicted_severity": predicted_severity,
                "probabilities": probs
            }
        elif severity_model is not None:
            predictions = severity_model.transform(input_df)
            result = predictions.select("prediction", "probability").collect()[0]
            return {
                "predicted_severity": int(result["prediction"]),
                "probabilities": result["probability"].toArray().tolist()
            }
        else:
            # Should not reach here because we check above, but just in case
            return {
                "predicted_severity": 2,
                "probabilities": [0.25, 0.25, 0.25, 0.25]
            }
    except Exception as e:
        print(f"Prediction error: {e}")
        return {
            "predicted_severity": 2,
            "probabilities": [0.15, 0.55, 0.20, 0.10]
        }

def create_prediction_consumer():
    """Create a Kafka/Redpanda consumer that reads prediction events.

    This consumer subscribes to the 'accident-predictions' topic produced by the
    Spark streaming job (kafka_streaming.py), which turns raw accidents into
    ML-scored severity predictions.
    """
    return KafkaConsumer(
        PREDICTION_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="accident-dashboard-predictions",
    )


def mock_streaming_worker():
    """Background worker that streams predictions from Kafka/Redpanda.

    Instead of generating accidents and scoring them in-memory, this worker
    now consumes from the 'accident-predictions' topic so all persistence is
    handled by Kafka/Redpanda.
    """
    global mock_streaming_active, streaming_predictions

    print("Prediction streaming worker consuming from Kafka/Redpanda")
    count = 0

    try:
        consumer = create_prediction_consumer()
    except Exception as e:
        print(f"Error creating prediction consumer: {e}")
        mock_streaming_active = False
        return

    try:
        for msg in consumer:
            if not mock_streaming_active:
                break

            try:
                event = msg.value
            except Exception as decode_err:
                print(f"Error decoding prediction event: {decode_err}")
                continue

            # Optional server-side state filter (All US vs specific state)
            state_filter = getattr(streaming_status, "current_state", None)
            if state_filter and event.get("state") != state_filter:
                continue

            streaming_predictions.append(event)
            count += 1

            sev = int(event.get("predicted_severity", 2))
            city = event.get("city") or event.get("neighborhood") or "Unknown"
            street = event.get("street") or ""
            weather = event.get("weather_condition", "Unknown")
            temp = event.get("temperature", "?")

            print(
                f"[{count}] Severity {sev} | id={event.get('id', 'N/A')} | "
                f"{city} - {street} | {weather} | {temp}°F"
            )
    except Exception as e:
        print(f"Error in streaming worker: {e}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        print("Prediction streaming worker stopped")

@app.route('/api/streaming/start', methods=['POST'])
def start_streaming():
    """Start the Kafka-based streaming prediction service.

    This will automatically spawn the raw accident producer and the Spark ML
    streaming job (kafka_streaming.py) as background processes if they are not
    already running, and then start the local consumer worker that feeds the
    React UI.
    """
    global mock_streaming_active, mock_streaming_thread, producer_process, ml_streaming_process

    if mock_streaming_active:
        return jsonify({'status': 'already_running', 'message': 'Streaming is already active'})

    # Auto-start Kafka producer (raw accidents) if not running
    try:
        if producer_process is None or producer_process.poll() is not None:
            print("Starting internal Kafka producer (kafka_producer.py)...")
            # Do not suppress stdout/stderr so we can see any connection/model errors
            producer_process = subprocess.Popen(
                [sys.executable, "kafka_producer.py", "--interval", "4"],
                cwd=SCRIPT_DIR,
            )
    except Exception as e:
        print(f"Warning: could not start kafka_producer.py automatically: {e}")

    # Auto-start ML streaming job if not running
    try:
        if ml_streaming_process is None or ml_streaming_process.poll() is not None:
            print("Starting internal ML streaming job (kafka_streaming.py)...")
            # Do not suppress stdout/stderr so we can see Spark/model/Kafka errors
            ml_streaming_process = subprocess.Popen(
                [sys.executable, "kafka_streaming.py"],
                cwd=SCRIPT_DIR,
            )
    except Exception as e:
        print(f"Warning: could not start kafka_streaming.py automatically: {e}")

    # Start local consumer that reads from accident-predictions and feeds the UI
    mock_streaming_active = True
    mock_streaming_thread = threading.Thread(target=mock_streaming_worker, daemon=True)
    mock_streaming_thread.start()

    return jsonify({
        'status': 'started',
        'message': 'Real-time streaming predictions started (Kafka + Spark ML)'
    })

@app.route('/api/streaming/stop', methods=['POST'])
def stop_streaming():
    """Stop the mock streaming prediction service"""
    global mock_streaming_active

    mock_streaming_active = False
    streaming_predictions.clear()

    return jsonify({
        'status': 'stopped',
        'message': 'Streaming stopped'
    })

@app.route('/api/streaming/status', methods=['GET'])
def streaming_status():
    """Get streaming status"""
    return jsonify({
        'active': mock_streaming_active,
        'predictions_count': len(streaming_predictions),
        'current_state': getattr(streaming_status, 'current_state', None)
    })

@app.route('/api/streaming/switch-state', methods=['POST'])
def switch_streaming_state():
    """Switch streaming to a specific state or all states"""
    global mock_streaming_active, streaming_predictions
    data = request.json or {}
    state_code = data.get('state')  # None = all states

    # Store current state filter
    streaming_status.current_state = state_code
    streaming_predictions.clear()

    state_name = state_code if state_code else "All US"
    return jsonify({
        'status': 'switched',
        'state': state_code,
        'message': f'Streaming switched to {state_name}'
    })

@app.route('/api/streaming/events')
def stream_events():
    """SSE endpoint for streaming predictions"""
    def generate():
        last_count = 0
        while True:
            if len(streaming_predictions) > last_count:
                # Send new predictions
                new_predictions = list(streaming_predictions)[last_count:]
                for pred in new_predictions:
                    yield f"data: {json.dumps(pred)}\n\n"
                last_count = len(streaming_predictions)

            # Send heartbeat every 5 seconds
            yield f": heartbeat\n\n"
            time.sleep(1)

    return Response(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*'
        }
    )

@app.route('/api/streaming/latest', methods=['GET'])
def get_latest_predictions():
    """Get the latest streaming predictions"""
    count = request.args.get('count', 20, type=int)
    return jsonify(list(streaming_predictions)[-count:])

# ============================================
# MANUAL PREDICTION ENDPOINT
# ============================================

@app.route('/api/predict', methods=['POST'])
def predict_severity():
    """Predict accident severity based on input features"""
    if (feature_model is None or not severity_models) and severity_model is None:
        return jsonify({'error': 'Model not loaded'}), 500

    try:
        data = request.json

        # Extract and calculate features
        latitude = float(data.get('latitude', 34.0522))
        longitude = float(data.get('longitude', -118.2437))
        hour_val = int(data.get('hour', 8))
        day_of_week = int(data.get('day_of_week', 3))
        temperature = float(data.get('temperature', 70))
        humidity = float(data.get('humidity', 50))
        pressure = float(data.get('pressure', 29.92))
        visibility = float(data.get('visibility', 10))
        wind_speed = float(data.get('wind_speed', 5))
        weather_condition = str(data.get('weather_condition', 'Clear'))
        sunrise_sunset = str(data.get('sunrise_sunset', 'Day'))
        crossing = float(data.get('crossing', 0))
        junction = float(data.get('junction', 0))
        traffic_signal = float(data.get('traffic_signal', 1))

        # Calculate derived features
        is_rush_hour = 1.0 if (7 <= hour_val <= 9) or (16 <= hour_val <= 19) else 0.0
        is_weekend = 1.0 if day_of_week in [1, 7] else 0.0
        time_of_day = 0.0 if sunrise_sunset == "Day" else 1.0
        month_val = float(datetime.now().month)

        if month_val in [12, 1, 2]:
            season = 0.0
        elif month_val in [3, 4, 5]:
            season = 1.0
        elif month_val in [6, 7, 8]:
            season = 2.0
        else:
            season = 3.0

        temp_humidity_interaction = temperature * humidity
        wind_visibility_interaction = wind_speed * visibility

        # Create input DataFrame
        input_data = [(
            latitude, longitude, float(hour_val), float(day_of_week),
            is_rush_hour, is_weekend, time_of_day, month_val, season,
            temperature, humidity, pressure, visibility, wind_speed,
            crossing, junction, traffic_signal,
            temp_humidity_interaction, wind_visibility_interaction,
            weather_condition, sunrise_sunset, 1.0
        )]

        schema = StructType([
            StructField("Start_Lat", DoubleType(), True),
            StructField("Start_Lng", DoubleType(), True),
            StructField("Hour", DoubleType(), True),
            StructField("DayOfWeek", DoubleType(), True),
            StructField("IsRushHour", DoubleType(), True),
            StructField("IsWeekend", DoubleType(), True),
            StructField("TimeOfDay", DoubleType(), True),
            StructField("Month", DoubleType(), True),
            StructField("Season", DoubleType(), True),
            StructField("Temperature(F)", DoubleType(), True),
            StructField("Humidity(%)", DoubleType(), True),
            StructField("Pressure(in)", DoubleType(), True),
            StructField("Visibility(mi)", DoubleType(), True),
            StructField("Wind_Speed(mph)", DoubleType(), True),
            StructField("Crossing", DoubleType(), True),
            StructField("Junction", DoubleType(), True),
            StructField("Traffic_Signal", DoubleType(), True),
            StructField("Temp_Humidity_Interaction", DoubleType(), True),
            StructField("Wind_Visibility_Interaction", DoubleType(), True),
            StructField("Weather_Condition", StringType(), True),
            StructField("Sunrise_Sunset", StringType(), True),
            StructField("classWeight", DoubleType(), True)
        ])

        input_df = spark.createDataFrame(input_data, schema)

        # Prefer One-vs-Rest models if available, otherwise fall back to legacy model
        if feature_model is not None and severity_models:
            severity_idx, probabilities = predict_severity_with_ovr(input_df)
            # Convert 0–3 index back to severity 1–4 for external consumers
            predicted_severity = severity_idx + 1
        else:
            predictions = severity_model.transform(input_df)
            result = predictions.select("prediction", "probability").collect()[0]
            predicted_severity = int(result["prediction"])
            probabilities = result["probability"].toArray().tolist()

        return jsonify({
            'predicted_severity': predicted_severity,
            'probabilities': probabilities,
            'input_summary': {
                'location': [latitude, longitude],
                'hour': hour_val,
                'day_of_week': day_of_week,
                'weather': weather_condition,
                'time_of_day': sunrise_sunset
            }
        })

    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500

if __name__ == '__main__':
    init_spark()
    app.run(host='0.0.0.0', port=5001, debug=False)

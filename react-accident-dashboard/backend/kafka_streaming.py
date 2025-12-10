"""
Kafka Consumer - State-Based Streaming with Dynamic Subscription
Subscribe to specific state topics when user clicks on state map
"""

import json
from collections import deque
from datetime import datetime

from kafka import KafkaProducer, KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
from pyspark.ml import PipelineModel

import os

# Kafka / Redpanda configuration (Kafka API)
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
PREDICTION_TOPIC = 'accident-predictions'

# Use the same PROJECT_ROOT layout as app.py so the model path is correct when
# kafka_streaming.py is started from the backend directory.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
MODEL_PATH = os.path.join(PROJECT_ROOT, 'accident_severity_model')

# (Legacy) in-memory storage – no longer used as primary pipeline, kept only for compatibility
predictions_queue = deque(maxlen=100)
alerts_queue = deque(maxlen=50)

# State
streaming_active = False
stream_query = None
current_state = None  # None = all states, "CA" = California only, etc.
spark = None
severity_model = None
kafka_producer = None

# Schema
accident_schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("state", StringType(), True),
    StructField("state_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("hour", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("visibility", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("weather_condition", StringType(), True),
    StructField("is_adverse_weather", BooleanType(), True),
    StructField("sunrise_sunset", StringType(), True),
    StructField("crossing", BooleanType(), True),
    StructField("junction", BooleanType(), True),
    StructField("traffic_signal", BooleanType(), True),
    StructField("weather_source", StringType(), True),
])


def init_spark():
    global spark, severity_model, kafka_producer
    
    print("Initializing Spark...")
    spark = SparkSession.builder \
        .appName("US Accident Streaming") \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load severity model (trained offline) for batch scoring
    try:
        severity_model = PipelineModel.load(MODEL_PATH)
        print("Model loaded")
    except Exception as e:
        print(f"Error loading model: {e}")
        return False

    # Initialize Kafka/Redpanda producer for prediction events
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        print(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return False

    return True


def score_and_publish(records):
    """Convert a list of accident event dicts to a Spark DataFrame, score
    them with the ML model, and publish predictions to Kafka/Redpanda.

    This keeps Spark for ML, but uses only plain KafkaConsumer/Producer for IO.
    """
    global predictions_queue, alerts_queue, severity_model, kafka_producer, spark

    if not records:
        return

    try:
        # Create Spark DataFrame from raw Kafka records
        # Explicitly cast numeric fields to float to satisfy DoubleType schema
        processed_records = []
        for rec in records:
            r = dict(rec)
            for field in ("temperature", "humidity", "pressure", "visibility", "wind_speed"):
                if field in r and r[field] is not None:
                    try:
                        r[field] = float(r[field])
                    except Exception:
                        pass
            processed_records.append(r)

        batch_df = spark.createDataFrame(processed_records, schema=accident_schema)

        df = batch_df.select(
            col("id"), col("timestamp"), col("state"), col("state_name"), col("city"),
            col("latitude").alias("Start_Lat"), col("longitude").alias("Start_Lng"),
            col("hour").cast("double").alias("Hour"),
            col("day_of_week").cast("double").alias("DayOfWeek"),
            col("temperature").alias("Temperature(F)"),
            col("humidity").alias("Humidity(%)"),
            col("pressure").alias("Pressure(in)"),
            col("visibility").alias("Visibility(mi)"),
            col("wind_speed").alias("Wind_Speed(mph)"),
            col("weather_condition").alias("Weather_Condition"),
            col("sunrise_sunset").alias("Sunrise_Sunset"),
            when(col("crossing"), 1.0).otherwise(0.0).alias("Crossing"),
            when(col("junction"), 1.0).otherwise(0.0).alias("Junction"),
            when(col("traffic_signal"), 1.0).otherwise(0.0).alias("Traffic_Signal"),
        )

        df = df.withColumn("IsRushHour",
            when((col("Hour") >= 7) & (col("Hour") <= 9), 1.0)
            .when((col("Hour") >= 16) & (col("Hour") <= 19), 1.0)
            .otherwise(0.0)
        ).withColumn("IsWeekend",
            when(col("DayOfWeek").isin([1, 7]), 1.0).otherwise(0.0)
        ).withColumn("TimeOfDay",
            when(col("Sunrise_Sunset") == "Day", 0.0).otherwise(1.0)
        ).withColumn("Month", lit(float(datetime.now().month))
        ).withColumn("Season",
            when(lit(datetime.now().month).isin([12, 1, 2]), 0.0)
            .when(lit(datetime.now().month).isin([3, 4, 5]), 1.0)
            .when(lit(datetime.now().month).isin([6, 7, 8]), 2.0)
            .otherwise(3.0)
        ).withColumn("Temp_Humidity_Interaction",
            col("Temperature(F)") * col("Humidity(%)")
        ).withColumn("Wind_Visibility_Interaction",
            col("Wind_Speed(mph)") * col("Visibility(mi)")
        ).withColumn("classWeight", lit(1.0))

        predictions = severity_model.transform(df)

        results = predictions.select(
            "id", "timestamp", "state", "state_name", "city",
            "Start_Lat", "Start_Lng", "Hour", "Weather_Condition",
            "Sunrise_Sunset", "Temperature(F)", "Visibility(mi)",
            "Wind_Speed(mph)", "prediction", "probability"
        ).collect()

        for row in results:
            severity = int(row["prediction"])

            result = {
                "id": row["id"],
                "timestamp": row["timestamp"],
                "state": row["state"],
                "state_name": row["state_name"],
                "city": row["city"],
                "latitude": row["Start_Lat"],
                "longitude": row["Start_Lng"],
                "hour": int(row["Hour"]),
                "weather_condition": row["Weather_Condition"],
                "time_of_day": row["Sunrise_Sunset"],
                "temperature": row["Temperature(F)"],
                "visibility": row["Visibility(mi)"],
                "wind_speed": row["Wind_Speed(mph)"],
                "predicted_severity": severity,
                "probabilities": row["probability"].toArray().tolist(),
                "processed_at": datetime.now().isoformat(),
                "weather_source": "OpenWeatherMap Live",
            }

            # Primary pipeline: publish to Kafka/Redpanda
            if kafka_producer is not None:
                try:
                    kafka_producer.send(PREDICTION_TOPIC, value=result)
                except Exception as send_err:
                    print(f"Error sending prediction to Kafka: {send_err}")

            # Optional in-process mirrors
            predictions_queue.append(result)
            if severity >= 3:
                alerts_queue.append({**result, "alert_level": "CRITICAL" if severity == 4 else "WARNING"})

            print(f"  [{row['state']}] {row['id']}: Severity {severity} | {row['city']}")

    except Exception as e:
        print(f"Error in score_and_publish: {e}")
        import traceback
        traceback.print_exc()


def get_topic_for_state(state_code):
    """Get Kafka topic for a state (None = all)"""
    if state_code is None:
        return "accident-all"
    return f"accident-{state_code.lower()}"


def start_streaming(state_code=None):
    """Start a plain KafkaConsumer loop and use Spark only for ML scoring.

    This replaces Spark Structured Streaming's Kafka source with a simple
    KafkaConsumer: accident-* topics -> score_and_publish -> accident-predictions.
    """
    global streaming_active, current_state

    if streaming_active:
        stop_streaming()

    if spark is None:
        if not init_spark():
            return False

    current_state = state_code
    topic = get_topic_for_state(state_code)

    try:
        print(f"Subscribing (KafkaConsumer) to topic: {topic}")
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=f"accident-ml-stream-{state_code or 'all'}",
        )
    except Exception as e:
        print(f"Error creating KafkaConsumer: {e}")
        return False

    streaming_active = True
    state_name = state_code if state_code else "All US"
    print(f"Streaming started for: {state_name} (KafkaConsumer + Spark ML)")

    try:
        # Process each message immediately to ensure predictions are produced
        for msg in consumer:
            if not streaming_active:
                break

            try:
                event = msg.value
            except Exception as decode_err:
                print(f"Error decoding Kafka message: {decode_err}")
                continue

            # Log Kafka offset/partition for observability
            try:
                print(
                    f"[KafkaConsumer] topic={msg.topic} partition={msg.partition} "
                    f"offset={msg.offset} id={event.get('id', 'N/A')}"
                )
            except Exception:
                # If event is not a dict, still log offset
                print(
                    f"[KafkaConsumer] topic={msg.topic} partition={msg.partition} "
                    f"offset={msg.offset} (id unavailable)"
                )

            # Score a single-event batch and publish prediction
            score_and_publish([event])

    except Exception as e:
        print(f"Error in streaming loop: {e}")
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        print("KafkaConsumer loop stopped")
        streaming_active = False
        current_state = None

    return True


def stop_streaming():
    """Signal the KafkaConsumer loop to stop."""
    global streaming_active, current_state
    streaming_active = False
    current_state = None
    print("Streaming stop requested")


def switch_state(state_code):
    """Switch to a different state's stream"""
    if state_code == current_state:
        return True
    return start_streaming(state_code)


def get_latest_predictions(count=50):
    return list(predictions_queue)[-count:]


def get_latest_alerts(count=10):
    return list(alerts_queue)[-count:]


def is_streaming():
    return streaming_active


def get_current_state():
    return current_state


if __name__ == "__main__":
    print("=" * 60)
    print("US State-Based Kafka Streaming (KafkaConsumer + Spark ML)")
    print("=" * 60)

    if init_spark():
        try:
            start_streaming(None)  # Start with all states
        except KeyboardInterrupt:
            stop_streaming()

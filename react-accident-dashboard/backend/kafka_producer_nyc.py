"""
Kafka Producer - NYC Mock Accident Data Generator
Sends mock NYC accident data with real weather to Kafka topics
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import requests

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Redpanda default port

# OpenWeatherMap API
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

# Weather cache
weather_cache = {}
cache_timestamp = {}
CACHE_DURATION = 300  # 5 minutes


def get_real_weather(lat, lng):
    """Fetch real weather data from OpenWeatherMap API"""
    cache_key = f"{lat:.1f},{lng:.1f}"
    now = time.time()

    if cache_key in weather_cache and (now - cache_timestamp.get(cache_key, 0)) < CACHE_DURATION:
        return weather_cache[cache_key]

    try:
        params = {"lat": lat, "lon": lng, "appid": OPENWEATHER_API_KEY, "units": "imperial"}
        response = requests.get(OPENWEATHER_URL, params=params, timeout=5)

        if response.status_code == 200:
            data = response.json()
            weather_data = {
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"] * 0.02953,
                "visibility": data.get("visibility", 10000) / 1609.34,
                "wind_speed": data["wind"]["speed"],
                "weather_condition": data["weather"][0]["main"],
            }
            condition_map = {"Clear": "Clear", "Clouds": "Cloudy", "Rain": "Rain",
                "Drizzle": "Light Rain", "Thunderstorm": "Thunderstorm",
                "Snow": "Snow", "Mist": "Fog", "Fog": "Fog"}
            weather_data["weather_condition"] = condition_map.get(
                weather_data["weather_condition"], weather_data["weather_condition"])
            weather_cache[cache_key] = weather_data
            cache_timestamp[cache_key] = now
            return weather_data
    except:
        pass

    return {"temperature": 55.0, "humidity": 50.0, "pressure": 29.92,
            "visibility": 10.0, "wind_speed": 5.0, "weather_condition": "Clear"}


def is_adverse_weather(condition):
    return condition in ["Rain", "Snow", "Fog", "Thunderstorm", "Light Rain"]


def generate_nyc_accident():
    """Generate mock accident data for NYC using REAL weather data"""
    location = random.choice(NYC_LOCATIONS)
    lat, lng, neighborhood, street = location

    # Add small randomness to exact location
    lat += random.uniform(-0.005, 0.005)
    lng += random.uniform(-0.005, 0.005)

    now = datetime.now()
    hour = now.hour
    day_of_week = 1 if now.isoweekday() == 7 else now.isoweekday() + 1

    # Get REAL weather data from API
    weather = get_real_weather(lat, lng)

    # Determine day/night
    sunrise_sunset = "Day" if 6 <= hour <= 18 else "Night"

    return {
        "id": f"ACC-NYC-{now.strftime('%H%M%S')}-{random.randint(100, 999)}",
        "timestamp": now.isoformat(),
        "state": "NY",
        "state_name": "New York",
        "city": "New York City",
        "latitude": round(lat, 6),
        "longitude": round(lng, 6),
        "neighborhood": neighborhood,
        "street": street,
        "hour": hour,
        "day_of_week": day_of_week,
        "temperature": round(weather["temperature"], 1),
        "humidity": round(weather["humidity"], 1),
        "pressure": round(weather["pressure"], 2),
        "visibility": round(min(weather["visibility"], 10), 1),
        "wind_speed": round(weather["wind_speed"], 1),
        "weather_condition": weather["weather_condition"],
        "is_adverse_weather": is_adverse_weather(weather["weather_condition"]),
        "sunrise_sunset": sunrise_sunset,
        "crossing": random.random() < 0.3,
        "junction": random.random() < 0.4,
        "traffic_signal": random.random() < 0.6,
        "weather_source": "OpenWeatherMap Live"
    }


def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            # Redpanda/Kafka connection settings
            request_timeout_ms=30000,
            retries=3
        )
        print(f"✓ Connected to Kafka/Redpanda at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        print(f"✗ Failed to connect to Kafka/Redpanda: {e}")
        print(f"  Make sure Redpanda is running: docker-compose up -d")
        return None


def run_producer(interval=8):
    """Run NYC accident data producer"""
    producer = create_producer()
    if not producer:
        print("Cannot start - ensure Kafka/Redpanda is running")
        return

    print("=" * 60)
    print("NYC Accident Data Producer - Kafka/Redpanda")
    print("=" * 60)
    print(f"\nTopic: accident-all (and accident-ny)")
    print(f"Interval: {interval} seconds")
    print("=" * 60 + "\n")

    count = 0

    try:
        while True:
            accident = generate_nyc_accident()

            # Send to main topic (accident-all)
            producer.send('accident-all', key=accident["id"], value=accident)

            # Also send to NY-specific topic
            producer.send('accident-ny', key=accident["id"], value=accident)

            # If adverse weather, send to weather topic
            if accident['is_adverse_weather']:
                producer.send('accident-weather-adverse', key=accident["id"], value=accident)

            count += 1
            print(f"[{count}] {accident['id']} | {accident['neighborhood']} - {accident['street']} | "
                  f"{accident['temperature']}°F | {accident['weather_condition']}")

            producer.flush()
            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n" + "=" * 60)
        print(f"Producer stopped. Total messages sent: {count}")
        print("=" * 60)
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', type=float, default=8, help='Seconds between messages')
    parser.add_argument('--bootstrap-servers', type=str, default='localhost:9092', help='Kafka bootstrap servers')
    args = parser.parse_args()

    if args.bootstrap_servers != 'localhost:9092':
        KAFKA_BOOTSTRAP_SERVERS = args.bootstrap_servers

    run_producer(args.interval)


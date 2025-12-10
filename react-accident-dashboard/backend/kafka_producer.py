"""
Kafka Producer - US-Wide Real-Time Accident Data Generator
Each state has its own Kafka topic for selective streaming
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import requests

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# OpenWeatherMap API
OPENWEATHER_API_KEY = "c5757baa8f23c85bedf0902235044704"
OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"

# All 50 US States with major cities/locations
US_LOCATIONS = {
    'AL': [('Birmingham', 33.5207, -86.8025), ('Mobile', 30.6954, -88.0399), ('Huntsville', 34.7304, -86.5861)],
    'AK': [('Anchorage', 61.2181, -149.9003), ('Fairbanks', 64.8378, -147.7164)],
    'AZ': [('Phoenix', 33.4484, -112.0740), ('Tucson', 32.2226, -110.9747), ('Mesa', 33.4152, -111.8315)],
    'AR': [('Little Rock', 34.7465, -92.2896), ('Fort Smith', 35.3859, -94.3985)],
    'CA': [('Los Angeles', 34.0522, -118.2437), ('San Francisco', 37.7749, -122.4194), ('San Diego', 32.7157, -117.1611), ('Sacramento', 38.5816, -121.4944), ('San Jose', 37.3382, -121.8863)],
    'CO': [('Denver', 39.7392, -104.9903), ('Colorado Springs', 38.8339, -104.8214), ('Aurora', 39.7294, -104.8319)],
    'CT': [('Hartford', 41.7658, -72.6734), ('New Haven', 41.3083, -72.9279), ('Bridgeport', 41.1865, -73.1952)],
    'DE': [('Wilmington', 39.7391, -75.5398), ('Dover', 39.1582, -75.5244)],
    'FL': [('Miami', 25.7617, -80.1918), ('Orlando', 28.5383, -81.3792), ('Tampa', 27.9506, -82.4572), ('Jacksonville', 30.3322, -81.6557)],
    'GA': [('Atlanta', 33.7490, -84.3880), ('Savannah', 32.0809, -81.0912), ('Augusta', 33.4735, -82.0105)],
    'HI': [('Honolulu', 21.3069, -157.8583), ('Hilo', 19.7297, -155.0900)],
    'ID': [('Boise', 43.6150, -116.2023), ('Idaho Falls', 43.4917, -112.0339)],
    'IL': [('Chicago', 41.8781, -87.6298), ('Springfield', 39.7817, -89.6501), ('Peoria', 40.6936, -89.5890)],
    'IN': [('Indianapolis', 39.7684, -86.1581), ('Fort Wayne', 41.0793, -85.1394), ('Evansville', 37.9716, -87.5711)],
    'IA': [('Des Moines', 41.5868, -93.6250), ('Cedar Rapids', 41.9779, -91.6656)],
    'KS': [('Wichita', 37.6872, -97.3301), ('Kansas City', 39.0997, -94.5786), ('Topeka', 39.0473, -95.6752)],
    'KY': [('Louisville', 38.2527, -85.7585), ('Lexington', 38.0406, -84.5037)],
    'LA': [('New Orleans', 29.9511, -90.0715), ('Baton Rouge', 30.4515, -91.1871), ('Shreveport', 32.5252, -93.7502)],
    'ME': [('Portland', 43.6591, -70.2568), ('Augusta', 44.3106, -69.7795)],
    'MD': [('Baltimore', 39.2904, -76.6122), ('Annapolis', 38.9784, -76.4922)],
    'MA': [('Boston', 42.3601, -71.0589), ('Worcester', 42.2626, -71.8023), ('Springfield', 42.1015, -72.5898)],
    'MI': [('Detroit', 42.3314, -83.0458), ('Grand Rapids', 42.9634, -85.6681), ('Ann Arbor', 42.2808, -83.7430)],
    'MN': [('Minneapolis', 44.9778, -93.2650), ('St. Paul', 44.9537, -93.0900), ('Rochester', 44.0121, -92.4802)],
    'MS': [('Jackson', 32.2988, -90.1848), ('Gulfport', 30.3674, -89.0928)],
    'MO': [('St. Louis', 38.6270, -90.1994), ('Kansas City', 39.0997, -94.5786), ('Springfield', 37.2090, -93.2923)],
    'MT': [('Billings', 45.7833, -108.5007), ('Missoula', 46.8721, -114.0193)],
    'NE': [('Omaha', 41.2565, -95.9345), ('Lincoln', 40.8136, -96.7026)],
    'NV': [('Las Vegas', 36.1699, -115.1398), ('Reno', 39.5296, -119.8138)],
    'NH': [('Manchester', 42.9956, -71.4548), ('Concord', 43.2081, -71.5376)],
    'NJ': [('Newark', 40.7357, -74.1724), ('Jersey City', 40.7178, -74.0431), ('Trenton', 40.2206, -74.7597)],
    'NM': [('Albuquerque', 35.0844, -106.6504), ('Santa Fe', 35.6870, -105.9378)],
    'NY': [('New York City', 40.7128, -74.0060), ('Buffalo', 42.8864, -78.8784), ('Albany', 42.6526, -73.7562), ('Rochester', 43.1566, -77.6088)],
    'NC': [('Charlotte', 35.2271, -80.8431), ('Raleigh', 35.7796, -78.6382), ('Greensboro', 36.0726, -79.7920)],
    'ND': [('Fargo', 46.8772, -96.7898), ('Bismarck', 46.8083, -100.7837)],
    'OH': [('Columbus', 39.9612, -82.9988), ('Cleveland', 41.4993, -81.6944), ('Cincinnati', 39.1031, -84.5120)],
    'OK': [('Oklahoma City', 35.4676, -97.5164), ('Tulsa', 36.1540, -95.9928)],
    'OR': [('Portland', 45.5152, -122.6784), ('Salem', 44.9429, -123.0351), ('Eugene', 44.0521, -123.0868)],
    'PA': [('Philadelphia', 39.9526, -75.1652), ('Pittsburgh', 40.4406, -79.9959), ('Harrisburg', 40.2732, -76.8867)],
    'RI': [('Providence', 41.8240, -71.4128), ('Newport', 41.4901, -71.3128)],
    'SC': [('Charleston', 32.7765, -79.9311), ('Columbia', 34.0007, -81.0348)],
    'SD': [('Sioux Falls', 43.5460, -96.7313), ('Rapid City', 44.0805, -103.2310)],
    'TN': [('Nashville', 36.1627, -86.7816), ('Memphis', 35.1495, -90.0490), ('Knoxville', 35.9606, -83.9207)],
    'TX': [('Houston', 29.7604, -95.3698), ('Dallas', 32.7767, -96.7970), ('Austin', 30.2672, -97.7431), ('San Antonio', 29.4241, -98.4936), ('El Paso', 31.7619, -106.4850)],
    'UT': [('Salt Lake City', 40.7608, -111.8910), ('Provo', 40.2338, -111.6585)],
    'VT': [('Burlington', 44.4759, -73.2121), ('Montpelier', 44.2601, -72.5754)],
    'VA': [('Richmond', 37.5407, -77.4360), ('Virginia Beach', 36.8529, -75.9780), ('Norfolk', 36.8508, -76.2859)],
    'WA': [('Seattle', 47.6062, -122.3321), ('Spokane', 47.6588, -117.4260), ('Tacoma', 47.2529, -122.4443)],
    'WV': [('Charleston', 38.3498, -81.6326), ('Huntington', 38.4192, -82.4452)],
    'WI': [('Milwaukee', 43.0389, -87.9065), ('Madison', 43.0731, -89.4012), ('Green Bay', 44.5133, -88.0133)],
    'WY': [('Cheyenne', 41.1400, -104.8202), ('Casper', 42.8666, -106.3131)],
    'DC': [('Washington', 38.9072, -77.0369)],
}

# State names for display
STATE_NAMES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
    'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
    'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
    'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
    'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
    'DC': 'Washington DC'
}

# Weather weights by state (some states have worse weather)
STATE_WEIGHTS = {
    'CA': 15, 'TX': 12, 'FL': 10, 'NY': 8, 'PA': 5, 'IL': 5, 'OH': 5, 
    'GA': 4, 'NC': 4, 'MI': 4, 'NJ': 4, 'VA': 4, 'WA': 3, 'AZ': 3,
    'MA': 3, 'TN': 3, 'IN': 3, 'MO': 3, 'MD': 3, 'WI': 2, 'CO': 2,
    'MN': 2, 'SC': 2, 'AL': 2, 'LA': 2, 'KY': 2, 'OR': 2, 'OK': 2,
    'CT': 2, 'IA': 1, 'MS': 1, 'AR': 1, 'UT': 1, 'KS': 1, 'NV': 1,
    'NM': 1, 'NE': 1, 'WV': 1, 'ID': 1, 'HI': 1, 'ME': 1, 'NH': 1,
    'RI': 1, 'MT': 1, 'DE': 1, 'SD': 1, 'AK': 1, 'ND': 1, 'DC': 1,
    'VT': 1, 'WY': 1
}

# Weather cache
weather_cache = {}
cache_timestamp = {}
CACHE_DURATION = 300


def get_state_topic(state_code):
    """Get Kafka topic name for a state"""
    return f"accident-{state_code.lower()}"


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


def generate_accident_data():
    """Generate accident data for random US location"""
    # Select state weighted by accident frequency
    states = list(STATE_WEIGHTS.keys())
    weights = list(STATE_WEIGHTS.values())
    state_code = random.choices(states, weights=weights)[0]
    
    # Select city in state
    city_data = random.choice(US_LOCATIONS[state_code])
    city, lat, lng = city_data
    
    # Add small random offset
    lat += random.uniform(-0.05, 0.05)
    lng += random.uniform(-0.05, 0.05)
    
    now = datetime.now()
    weather = get_real_weather(lat, lng)
    
    return {
        "id": f"ACC-{state_code}-{now.strftime('%H%M%S')}-{random.randint(100, 999)}",
        "timestamp": now.isoformat(),
        "state": state_code,
        "state_name": STATE_NAMES[state_code],
        "city": city,
        "latitude": round(lat, 6),
        "longitude": round(lng, 6),
        "hour": now.hour,
        "day_of_week": 1 if now.isoweekday() == 7 else now.isoweekday() + 1,
        "temperature": round(weather["temperature"], 1),
        "humidity": round(weather["humidity"], 1),
        "pressure": round(weather["pressure"], 2),
        "visibility": round(min(weather["visibility"], 10), 1),
        "wind_speed": round(weather["wind_speed"], 1),
        "weather_condition": weather["weather_condition"],
        "is_adverse_weather": is_adverse_weather(weather["weather_condition"]),
        "sunrise_sunset": "Day" if 6 <= now.hour <= 18 else "Night",
        "crossing": random.random() < 0.3,
        "junction": random.random() < 0.4,
        "traffic_signal": random.random() < 0.6,
        "weather_source": "OpenWeatherMap"
    }


def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return None


def run_producer(interval=4):
    """Run US-wide multi-topic producer"""
    producer = create_producer()
    if not producer:
        print("Cannot start - ensure Kafka is running")
        return
    
    print("=" * 60)
    print("US-Wide Kafka Producer - State-Based Topics")
    print("=" * 60)
    print("\nTOPICS (51 state topics + 3 special):")
    print("  Per State: accident-{state_code} (e.g., accident-ca, accident-ny)")
    print("  Weather:   accident-weather-adverse")
    print("  All:       accident-all")
    print("  Alerts:    accident-alerts-high")
    print("=" * 60 + "\n")
    
    counts = {}
    
    try:
        while True:
            accident = generate_accident_data()
            state_code = accident['state']
            
            # 1. Send to state-specific topic
            state_topic = get_state_topic(state_code)
            producer.send(state_topic, key=accident["id"], value=accident)
            counts[state_topic] = counts.get(state_topic, 0) + 1
            
            # 2. Send to ALL topic
            producer.send('accident-all', key=accident["id"], value=accident)
            counts['accident-all'] = counts.get('accident-all', 0) + 1
            
            # 3. If adverse weather, send to weather topic
            if accident['is_adverse_weather']:
                producer.send('accident-weather-adverse', key=accident["id"], value=accident)
                counts['accident-weather-adverse'] = counts.get('accident-weather-adverse', 0) + 1
            
            print(f"[{state_topic}] {accident['id']} | {accident['city']}, {state_code} | "
                  f"{accident['temperature']}°F | {accident['weather_condition']}")
            
            producer.flush()
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print(f"\n" + "=" * 60)
        print("Producer stopped. Counts by state:")
        for topic in sorted(counts.keys()):
            if counts[topic] > 0:
                print(f"  {topic}: {counts[topic]}")
        print("=" * 60)
    finally:
        producer.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--interval', type=float, default=4)
    args = parser.parse_args()
    run_producer(args.interval)

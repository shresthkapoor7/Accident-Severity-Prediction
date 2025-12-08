import json
import random
from datetime import datetime, timedelta

# Boroughs and their corresponding data
BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
BOROUGHS_UPPER = [b.upper() for b in BOROUGHS]

# Event types and agencies
EVENT_TYPES = ["Special Event", "Sport - Adult", "Sport - Youth", "Cultural Event", "Street Fair", "Parade"]
EVENT_AGENCIES = ["Parks Department", "NYC DOT", "NYPD", "Department of Cultural Affairs"]

# Vehicle types
VEHICLE_TYPES = ["Sedan", "SUV", "Truck", "Van", "Motorcycle", "Bus", "Taxi", "Carry All", "Station Wagon/Sport Utility Vehicle", "Tractor Truck Diesel"]

# Contributing factors
CONTRIBUTING_FACTORS = [
    "Unsafe Lane Changing", "Backing Unsafely", "Following Too Closely", 
    "Unspecified", "Driver Inattention/Distraction", "Failure to Yield Right-of-Way",
    "Alcohol Involvement", "Passing or Lane Usage Improper", "Turning Improperly"
]

# Weather conditions
WEATHER_MAIN = ["Clear", "Clouds", "Rain", "Snow", "Mist", "Fog"]
WEATHER_DESCRIPTIONS = {
    "Clear": ["clear sky"],
    "Clouds": ["few clouds", "scattered clouds", "broken clouds", "overcast clouds"],
    "Rain": ["light rain", "moderate rain", "heavy intensity rain"],
    "Snow": ["light snow", "moderate snow"],
    "Mist": ["mist"],
    "Fog": ["fog"]
}

# NYC zip codes by borough
ZIP_CODES = {
    "Manhattan": ["10001", "10002", "10003", "10004", "10005", "10006", "10007", "10009", "10010", "10011", "10012", "10013", "10014", "10016", "10017", "10018", "10019", "10020", "10021", "10022", "10023", "10024", "10025", "10026", "10027", "10028", "10029", "10030", "10031", "10032", "10033", "10034", "10035", "10036", "10037", "10038", "10039", "10040", "10044", "10065", "10069", "10075", "10128"],
    "Brooklyn": ["11201", "11202", "11203", "11204", "11205", "11206", "11207", "11208", "11209", "11210", "11211", "11212", "11213", "11214", "11215", "11216", "11217", "11218", "11219", "11220", "11221", "11222", "11223", "11224", "11225", "11226", "11228", "11229", "11230", "11231", "11232", "11233", "11234", "11235", "11236", "11237", "11238", "11239"],
    "Queens": ["11101", "11102", "11103", "11104", "11105", "11106", "11354", "11355", "11356", "11357", "11358", "11359", "11360", "11361", "11362", "11363", "11364", "11365", "11366", "11367", "11368", "11369", "11370", "11371", "11372", "11373", "11374", "11375", "11377", "11378", "11379", "11385", "11411", "11412", "11413", "11414", "11415", "11416", "11417", "11418", "11419", "11420", "11421", "11422", "11423", "11424", "11425", "11426", "11427", "11428", "11429", "11430", "11432", "11433", "11434", "11435", "11436"],
    "Bronx": ["10451", "10452", "10453", "10454", "10455", "10456", "10457", "10458", "10459", "10460", "10461", "10462", "10463", "10464", "10465", "10466", "10467", "10468", "10469", "10470", "10471", "10472", "10473", "10474", "10475"],
    "Staten Island": ["10301", "10302", "10303", "10304", "10305", "10306", "10307", "10308", "10309", "10310", "10311", "10312", "10314"]
}

# NYC coordinates by borough (approximate centers)
BOROUGH_COORDS = {
    "Manhattan": {"lat": 40.7831, "lon": -73.9712},
    "Brooklyn": {"lat": 40.6782, "lon": -73.9442},
    "Queens": {"lat": 40.7282, "lon": -73.7949},
    "Bronx": {"lat": 40.8448, "lon": -73.8648},
    "Staten Island": {"lat": 40.5795, "lon": -74.1502}
}

def generate_lat_lon(borough):
    """Generate random lat/lon within borough bounds"""
    base = BOROUGH_COORDS[borough]
    # Add small random offset
    lat = base["lat"] + random.uniform(-0.1, 0.1)
    lon = base["lon"] + random.uniform(-0.1, 0.1)
    return round(lat, 5), round(lon, 5)

def generate_time():
    """Generate random time in HH:MM format"""
    hour = random.randint(0, 23)
    minute = random.choice([0, 15, 30, 45])
    return f"{hour:02d}:{minute:02d}"

def generate_date(start_date="2025-11-01", end_date="2025-12-31"):
    """Generate random date between start and end"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end - start
    random_days = random.randint(0, delta.days)
    date = start + timedelta(days=random_days)
    return date.strftime("%Y-%m-%dT00:00:00.000")

# Street names for NYC
STREET_NAMES = [
    "Broadway", "5th Avenue", "Park Avenue", "Lexington Avenue", "Madison Avenue",
    "Wall Street", "Main Street", "1st Street", "2nd Street", "3rd Street",
    "Washington Street", "Lincoln Avenue", "Roosevelt Avenue", "Columbus Avenue",
    "Amsterdam Avenue", "West End Avenue", "Riverside Drive", "Central Park West"
]

CITY_NAMES = ["New York", "Brooklyn", "Queens", "Manhattan", "Bronx", "Staten Island"]

def generate_event_name(event_type):
    """Generate event name based on type"""
    if "Sport" in event_type:
        sports = ["Soccer", "Football", "Basketball", "Baseball", "Tennis", "Volleyball"]
        return f"{random.choice(sports)} - {'Regulation' if random.random() > 0.5 else 'Non Regulation'}"
    elif event_type == "Special Event":
        events = ["Community Gathering", "Cultural Festival", "Art Exhibition", "Music Concert", "Food Festival"]
        return random.choice(events)
    elif event_type == "Street Fair":
        return f"{random.choice(STREET_NAMES)} Street Fair"
    elif event_type == "Parade":
        return f"{random.choice(CITY_NAMES)} Parade"
    else:
        return f"{random.choice(['Annual', 'Spring', 'Summer', 'Winter', 'Fall'])} {random.choice(['Festival', 'Celebration', 'Event'])}"

def generate_weather_data(date_str):
    """Generate weather data for a given date"""
    main = random.choice(WEATHER_MAIN)
    description = random.choice(WEATHER_DESCRIPTIONS[main])
    
    # Temperature varies by season (assuming winter for Nov-Dec)
    temp = random.uniform(-5, 10)
    feels_like = temp - random.uniform(1, 5)
    humidity = random.randint(30, 95)
    pressure = random.randint(1015, 1035)
    wind_speed = random.uniform(1, 10)
    wind_deg = random.randint(0, 360)
    clouds = random.randint(0, 100)
    visibility = random.choice([5000, 10000])
    
    return {
        "main": main,
        "description": description,
        "temp": round(temp, 2),
        "feels_like": round(feels_like, 2),
        "humidity": humidity,
        "pressure": pressure,
        "wind_speed": round(wind_speed, 2),
        "wind_deg": wind_deg,
        "clouds": clouds,
        "visibility": visibility
    }

def generate_mock_joined_dataset(num_rows=100):
    """Generate mock dataset with joined data from all three sources"""
    dataset = []
    
    for i in range(num_rows):
        # Generate base date
        crash_date = generate_date()
        crash_time = generate_time()
        borough = random.choice(BOROUGHS)
        borough_upper = borough.upper()
        zip_code = random.choice(ZIP_CODES[borough])
        lat, lon = generate_lat_lon(borough)
        
        # Generate collision data
        collision_id = str(random.randint(4000000, 5000000))
        num_injured = random.randint(0, 5)
        num_killed = random.randint(0, 2)
        num_ped_injured = random.randint(0, min(3, num_injured))
        num_ped_killed = random.randint(0, min(1, num_killed))
        num_cyclist_injured = random.randint(0, min(2, num_injured - num_ped_injured))
        num_cyclist_killed = random.randint(0, min(1, num_killed - num_ped_killed))
        num_motorist_injured = num_injured - num_ped_injured - num_cyclist_injured
        num_motorist_killed = num_killed - num_ped_killed - num_cyclist_killed
        
        vehicle_type_1 = random.choice(VEHICLE_TYPES)
        vehicle_type_2 = random.choice(VEHICLE_TYPES) if random.random() > 0.3 else None
        
        contributing_factor_1 = random.choice(CONTRIBUTING_FACTORS)
        contributing_factor_2 = random.choice(CONTRIBUTING_FACTORS) if random.random() > 0.4 else None
        contributing_factor_3 = random.choice(CONTRIBUTING_FACTORS) if random.random() > 0.7 else None
        
        on_street = random.choice(STREET_NAMES).upper()
        off_street = random.choice(STREET_NAMES).upper() if random.random() > 0.2 else None
        cross_street = random.choice(STREET_NAMES).upper() if random.random() > 0.5 else None
        
        # Generate event data (may or may not exist for this date/location)
        has_event = random.random() > 0.6  # 40% chance of having an event
        
        if has_event:
            event_id = str(random.randint(800000, 900000))
            event_type = random.choice(EVENT_TYPES)
            event_name = generate_event_name(event_type)
            event_agency = random.choice(EVENT_AGENCIES)
            
            # Event date should be close to crash date
            event_date_obj = datetime.strptime(crash_date, "%Y-%m-%dT00:00:00.000")
            start_offset = random.randint(-2, 2)
            event_start = event_date_obj + timedelta(days=start_offset)
            event_end = event_start + timedelta(hours=random.randint(2, 8))
            
            event_start_str = event_start.strftime("%Y-%m-%dT%H:%M:00.000")
            event_end_str = event_end.strftime("%Y-%m-%dT%H:%M:00.000")
            
            event_location = f"{random.choice(['Park', 'Street', 'Avenue', 'Plaza'])}: {random.choice(STREET_NAMES)}"
            community_board = str(random.randint(1, 18))
            police_precinct = str(random.randint(1, 123))
            street_closure_type = random.choice(["N/A", "Full Closure", "Partial Closure", "Lane Closure"])
        else:
            event_id = None
            event_name = None
            event_type = None
            event_agency = None
            event_start_str = None
            event_end_str = None
            event_location = None
            community_board = None
            police_precinct = None
            street_closure_type = None
        
        # Generate weather data
        weather = generate_weather_data(crash_date)
        
        # Create joined record
        record = {
            # Collision data
            "collision_id": collision_id,
            "crash_date": crash_date,
            "crash_time": crash_time,
            "borough": borough_upper,
            "zip_code": zip_code,
            "latitude": str(lat),
            "longitude": str(lon),
            "on_street_name": on_street,
            "off_street_name": off_street,
            "cross_street_name": cross_street,
            "number_of_persons_injured": str(num_injured),
            "number_of_persons_killed": str(num_killed),
            "number_of_pedestrians_injured": str(num_ped_injured),
            "number_of_pedestrians_killed": str(num_ped_killed),
            "number_of_cyclist_injured": str(num_cyclist_injured),
            "number_of_cyclist_killed": str(num_cyclist_killed),
            "number_of_motorist_injured": str(num_motorist_injured),
            "number_of_motorist_killed": str(num_motorist_killed),
            "contributing_factor_vehicle_1": contributing_factor_1,
            "contributing_factor_vehicle_2": contributing_factor_2,
            "contributing_factor_vehicle_3": contributing_factor_3,
            "vehicle_type_code1": vehicle_type_1,
            "vehicle_type_code2": vehicle_type_2,
            
            # Event data
            "event_id": event_id,
            "event_name": event_name,
            "event_type": event_type,
            "event_agency": event_agency,
            "event_borough": borough if has_event else None,
            "event_location": event_location,
            "event_start_date_time": event_start_str,
            "event_end_date_time": event_end_str,
            "street_closure_type": street_closure_type,
            "community_board": community_board,
            "police_precinct": police_precinct,
            
            # Weather data
            "weather_main": weather["main"],
            "weather_description": weather["description"],
            "temperature": weather["temp"],
            "feels_like": weather["feels_like"],
            "humidity": weather["humidity"],
            "pressure": weather["pressure"],
            "wind_speed": weather["wind_speed"],
            "wind_deg": weather["wind_deg"],
            "clouds": weather["clouds"],
            "visibility": weather["visibility"]
        }
        
        dataset.append(record)
    
    return dataset

def save_dataset_to_json(dataset, filename="mock_joined_dataset.json"):
    """Save dataset to JSON file"""
    with open(filename, 'w') as f:
        json.dump(dataset, f, indent=2)
    print(f"Dataset saved to {filename} with {len(dataset)} rows")

def save_dataset_to_csv(dataset, filename="mock_joined_dataset.csv"):
    """Save dataset to CSV file"""
    import csv
    
    if not dataset:
        return
    
    fieldnames = dataset[0].keys()
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(dataset)
    print(f"Dataset saved to {filename} with {len(dataset)} rows")

if __name__ == "__main__":
    print("Generating mock joined dataset with 100 rows...")
    dataset = generate_mock_joined_dataset(100)
    
    # Save to JSON
    save_dataset_to_json(dataset, "mock_joined_dataset.json")
    
    # Save to CSV
    save_dataset_to_csv(dataset, "mock_joined_dataset.csv")
    
    # Print first record as example
    print("\nFirst record example:")
    print(json.dumps(dataset[0], indent=2))
    
    print(f"\nTotal records generated: {len(dataset)}")


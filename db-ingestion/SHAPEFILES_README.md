# NYC Shapefiles Ingestion Guide

This script ingests LION (Street Centerlines) and Taxi Zones shapefiles into MongoDB.

## Manual Download Instructions

Since direct download URLs may change, you'll need to manually download the shapefiles:

### 1. LION (Street Centerlines) Shapefile

**Source**: [NYC Planning - LION Dataset](https://www.nyc.gov/content/planning/pages/resources/datasets/lion)

**Steps**:
1. Visit: https://www.nyc.gov/content/planning/pages/resources/datasets/lion
2. Click on the download link for the latest LION shapefile
3. Save the ZIP file (e.g., `nyclion_24a.zip`)

### 2. Taxi Zones Shapefile

**Source**: [NYC Open Data - Taxi Zones](https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddvh)

**Steps**:
1. Visit: https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddvh
2. Click "Export" → "Shapefile"
3. Save the ZIP file (e.g., `taxi_zones.zip`)

## Usage

### Option 1: Using Local Files (Recommended)

After downloading the shapefiles, run:

```bash
python load_shapefiles.py --lion-file /path/to/nyclion_24a.zip --taxi-zones-file /path/to/taxi_zones.zip
```

Or just one at a time:

```bash
# LION only
python load_shapefiles.py --lion-file /path/to/nyclion_24a.zip

# Taxi Zones only
python load_shapefiles.py --taxi-zones-file /path/to/taxi_zones.zip
```

### Option 2: Automatic Download (May Not Work)

The script will try to download automatically, but URLs may be outdated:

```bash
python load_shapefiles.py
```

## What the Script Does

1. **Downloads/Extracts**: Downloads shapefiles or extracts from local ZIP files
2. **Converts to GeoJSON**: Converts shapefile geometries to GeoJSON format
3. **Ingests to MongoDB**: 
   - LION → `lion_streets` collection
   - Taxi Zones → `taxi_zones` collection
4. **Creates Geospatial Indexes**: Creates 2dsphere indexes for efficient geospatial queries

## MongoDB Collections

After ingestion, you'll have:

- **`lion_streets`**: Street centerlines with geometry and attributes
- **`taxi_zones`**: Taxi zone boundaries with geometry and zone information

## Example Queries

### Find streets near a collision location:

```python
from pymongo import MongoClient
from bson import SON

client = MongoClient('localhost', 27017)
db = client['nyc_collisions']

# Find streets within 100 meters of a point
point = {"type": "Point", "coordinates": [-73.9712, 40.7831]}
streets = db.lion_streets.find({
    "geometry": {
        "$near": {
            "$geometry": point,
            "$maxDistance": 100
        }
    }
})
```

### Find taxi zone for a location:

```python
# Find which taxi zone contains a point
point = {"type": "Point", "coordinates": [-73.9712, 40.7831]}
zone = db.taxi_zones.find_one({
    "geometry": {
        "$geoIntersects": {
            "$geometry": point
        }
    }
})
```

## Requirements

```bash
pip install -r requirements.txt
```

Required packages:
- `geopandas` - For reading shapefiles
- `shapely` - For geometry operations
- `pymongo` - For MongoDB operations
- `requests` - For downloading (if using auto-download)

## Troubleshooting

### "geopandas not installed"
```bash
pip install geopandas shapely
```

### "Could not find .shp file"
Make sure the ZIP file contains a `.shp` file. Some downloads may have nested directories.

### "MongoDB connection failed"
Make sure MongoDB is running:
```bash
docker ps | grep mongodb
# If not running:
docker start mongodb
```

## Data Sources

- **LION**: NYC Department of City Planning - https://www.nyc.gov/content/planning/pages/resources/datasets/lion
- **Taxi Zones**: NYC Open Data - https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddvh


"""
NYC Shapefiles Ingestion Script
Downloads LION (Street Centerlines) and Taxi Zones shapefiles and ingests into MongoDB
"""

import requests
import zipfile
import os
import tempfile
from pymongo import MongoClient, GEOSPHERE
from typing import Dict, List, Optional
import json
import sys

# MongoDB Configuration
MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_DB_NAME = "nyc_collisions"

# Shapefile URLs
# LION: https://www.nyc.gov/content/planning/pages/resources/datasets/lion
LION_SHAPEFILE_URLS = [
    "https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/nyclion_24a.zip",
    "https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/nyclion_23d.zip",
    "https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/nyclion_23c.zip",
    "https://www1.nyc.gov/assets/planning/download/zip/data-maps/open-data/nyclion.zip"
]

# Taxi Zones: NYC Open Data
TAXI_ZONES_SHAPEFILE_URLS = [
    "https://data.cityofnewyork.us/api/geospatial/d3c5-ddvh?method=export&format=Shapefile",
    "https://data.cityofnewyork.us/download/d3c5-ddvh/application%2Fzip",
    "https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip"
]

class ShapefileDownloader:
    """Class to handle downloading shapefiles"""
    
    def __init__(self):
        self.temp_dir = None
    
    def download_shapefile(self, url: str, filename: str) -> str:
        """
        Download a shapefile zip from URL
        
        Args:
            url: URL to download from
            filename: Name for the downloaded file
        
        Returns:
            Path to downloaded zip file
        """
        print(f"Downloading {filename} from {url}...")
        
        try:
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            # Create temp directory if needed
            if not self.temp_dir:
                self.temp_dir = tempfile.mkdtemp()
            
            zip_path = os.path.join(self.temp_dir, filename)
            
            # Download with progress
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            print(f"\r  Progress: {percent:.1f}%", end='', flush=True)
            
            print(f"\n✓ Successfully downloaded {filename} ({downloaded / (1024*1024):.1f} MB)")
            return zip_path
            
        except requests.exceptions.RequestException as e:
            print(f"\n✗ Error downloading {filename}: {e}")
            raise
    
    def extract_shapefile(self, zip_path: str, extract_to: str) -> str:
        """
        Extract shapefile from zip
        
        Args:
            zip_path: Path to zip file
            extract_to: Directory to extract to
        
        Returns:
            Path to extracted shapefile directory
        """
        print(f"Extracting {os.path.basename(zip_path)}...")
        
        try:
            os.makedirs(extract_to, exist_ok=True)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            
            print(f"✓ Extracted to {extract_to}")
            return extract_to
            
        except Exception as e:
            print(f"✗ Error extracting: {e}")
            raise
    
    def cleanup(self):
        """Clean up temporary files"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)
            print("✓ Cleaned up temporary files")

class ShapefileIngester:
    """Class to handle ingesting shapefiles into MongoDB"""
    
    def __init__(self, host: str = MONGO_HOST, port: int = MONGO_PORT, 
                 db_name: str = MONGO_DB_NAME):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.client = None
        self.db = None
    
    def connect(self) -> bool:
        """Connect to MongoDB"""
        try:
            print(f"\nConnecting to MongoDB at {self.host}:{self.port}...")
            self.client = MongoClient(self.host, self.port, serverSelectionTimeoutMS=5000)
            self.client.server_info()
            self.db = self.client[self.db_name]
            print(f"✓ Successfully connected to MongoDB")
            print(f"  Database: {self.db_name}")
            return True
        except Exception as e:
            print(f"✗ Error connecting to MongoDB: {e}")
            return False
    
    def ingest_shapefile_geopandas(self, shapefile_path: str, collection_name: str):
        """
        Ingest shapefile using geopandas (preferred method)
        
        Args:
            shapefile_path: Path to shapefile (.shp file)
            collection_name: MongoDB collection name
        """
        try:
            import geopandas as gpd
            from shapely.geometry import mapping
            
            print(f"\nReading shapefile: {shapefile_path}")
            gdf = gpd.read_file(shapefile_path)
            
            print(f"✓ Loaded {len(gdf)} features")
            print(f"  Columns: {list(gdf.columns)}")
            print(f"  CRS: {gdf.crs}")
            
            # Convert to GeoJSON format for MongoDB
            collection = self.db[collection_name]
            
            # Clear existing data
            result = collection.delete_many({})
            print(f"  Cleared {result.deleted_count} existing documents")
            
            # Convert to GeoJSON and insert
            print(f"\nConverting to GeoJSON and inserting into MongoDB...")
            batch_size = 1000
            total_inserted = 0
            
            for i in range(0, len(gdf), batch_size):
                batch = gdf.iloc[i:i+batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(gdf) + batch_size - 1) // batch_size
                
                documents = []
                for idx, row in batch.iterrows():
                    # Convert geometry to GeoJSON
                    geom = mapping(row.geometry)
                    
                    # Create document
                    doc = row.drop('geometry').to_dict()
                    doc['geometry'] = geom
                    doc['type'] = 'Feature'
                    
                    # Clean up None values
                    doc = {k: v for k, v in doc.items() if v is not None}
                    
                    documents.append(doc)
                
                if documents:
                    result = collection.insert_many(documents, ordered=False)
                    total_inserted += len(result.inserted_ids)
                    print(f"  Batch {batch_num}/{total_batches}: Inserted {len(result.inserted_ids)} documents")
            
            # Create geospatial index
            print(f"\nCreating geospatial index...")
            collection.create_index([("geometry", GEOSPHERE)])
            print(f"✓ Geospatial index created")
            
            print(f"\n✓ Successfully ingested {total_inserted} documents into '{collection_name}' collection")
            return total_inserted
            
        except ImportError:
            print("✗ geopandas not installed. Trying alternative method...")
            return self.ingest_shapefile_pyshp(shapefile_path, collection_name)
        except Exception as e:
            print(f"✗ Error ingesting with geopandas: {e}")
            print("Trying alternative method...")
            return self.ingest_shapefile_pyshp(shapefile_path, collection_name)
    
    def ingest_shapefile_pyshp(self, shapefile_path: str, collection_name: str):
        """
        Ingest shapefile using pyshp (alternative method)
        
        Args:
            shapefile_path: Path to shapefile (.shp file)
            collection_name: MongoDB collection name
        """
        try:
            import shapefile
            
            print(f"\nReading shapefile with pyshp: {shapefile_path}")
            sf = shapefile.Reader(shapefile_path)
            
            print(f"✓ Loaded shapefile with {len(sf)} features")
            
            # Get field names
            fields = [field[0] for field in sf.fields[1:]]  # Skip deletion field
            
            collection = self.db[collection_name]
            
            # Clear existing data
            result = collection.delete_many({})
            print(f"  Cleared {result.deleted_count} existing documents")
            
            # Convert and insert
            print(f"\nConverting to GeoJSON and inserting into MongoDB...")
            batch_size = 1000
            documents = []
            total_inserted = 0
            batch_num = 0
            
            for i, shape_record in enumerate(sf.iterShapeRecords()):
                shape = shape_record.shape
                record = shape_record.record
                
                # Convert shape to GeoJSON
                if shape.shapeType == 1:  # Point
                    geom = {
                        "type": "Point",
                        "coordinates": [shape.points[0][0], shape.points[0][1]]
                    }
                elif shape.shapeType == 3:  # Polyline
                    geom = {
                        "type": "LineString",
                        "coordinates": shape.points
                    }
                elif shape.shapeType == 5:  # Polygon
                    if len(shape.parts) > 1:
                        geom = {
                            "type": "MultiPolygon",
                            "coordinates": [shape.points]
                        }
                    else:
                        geom = {
                            "type": "Polygon",
                            "coordinates": [shape.points]
                        }
                else:
                    continue  # Skip unsupported geometry types
                
                # Create document
                doc = {"geometry": geom, "type": "Feature"}
                
                # Add attributes
                for j, field in enumerate(fields):
                    if j < len(record):
                        doc[field] = record[j]
                
                documents.append(doc)
                
                # Insert in batches
                if len(documents) >= batch_size:
                    batch_num += 1
                    result = collection.insert_many(documents, ordered=False)
                    total_inserted += len(result.inserted_ids)
                    print(f"  Batch {batch_num}: Inserted {len(result.inserted_ids)} documents")
                    documents = []
            
            # Insert remaining documents
            if documents:
                batch_num += 1
                result = collection.insert_many(documents, ordered=False)
                total_inserted += len(result.inserted_ids)
                print(f"  Batch {batch_num}: Inserted {len(result.inserted_ids)} documents")
            
            # Create geospatial index
            print(f"\nCreating geospatial index...")
            collection.create_index([("geometry", GEOSPHERE)])
            print(f"✓ Geospatial index created")
            
            print(f"\n✓ Successfully ingested {total_inserted} documents into '{collection_name}' collection")
            return total_inserted
            
        except ImportError:
            print("✗ pyshp not installed. Please install either geopandas or pyshp:")
            print("  pip install geopandas")
            print("  OR")
            print("  pip install pyshp")
            raise
        except Exception as e:
            print(f"✗ Error ingesting with pyshp: {e}")
            raise
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            print("✓ MongoDB connection closed")

def find_shapefile_in_directory(directory: str) -> Optional[str]:
    """Find .shp file in directory"""
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.shp'):
                return os.path.join(root, file)
    return None

def main():
    """Main function to download and ingest shapefiles"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Download and ingest NYC shapefiles')
    parser.add_argument('--lion-file', type=str, default=None,
                       help='Path to local LION shapefile (.shp or .zip)')
    parser.add_argument('--taxi-zones-file', type=str, default=None,
                       help='Path to local Taxi Zones shapefile (.shp or .zip)')
    args = parser.parse_args()
    
    print("="*80)
    print("NYC Shapefiles Ingestion: LION & Taxi Zones")
    print("="*80)
    
    downloader = ShapefileDownloader()
    ingester = ShapefileIngester()
    
    try:
        # Connect to MongoDB
        if not ingester.connect():
            print("✗ Failed to connect to MongoDB. Exiting.")
            return
        
        # Download and ingest LION shapefile
        print("\n" + "="*80)
        print("LION (Street Centerlines) Shapefile")
        print("="*80)
        print("Source: https://www.nyc.gov/content/planning/pages/resources/datasets/lion")
        
        lion_success = False
        
        # Check if local file provided
        if args.lion_file and os.path.exists(args.lion_file):
            print(f"\nUsing local LION file: {args.lion_file}")
            try:
                if args.lion_file.endswith('.zip'):
                    lion_extract = os.path.join(downloader.temp_dir, "lion_local")
                    downloader.extract_shapefile(args.lion_file, lion_extract)
                    lion_shp = find_shapefile_in_directory(lion_extract)
                else:
                    lion_shp = args.lion_file
                
                if lion_shp and os.path.exists(lion_shp):
                    ingester.ingest_shapefile_geopandas(lion_shp, "lion_streets")
                    lion_success = True
                else:
                    print("✗ Could not find .shp file")
            except Exception as e:
                print(f"✗ Error processing local LION file: {e}")
        else:
            # Try downloading from URLs
            for i, url in enumerate(LION_SHAPEFILE_URLS, 1):
                try:
                    print(f"\nTrying LION URL {i}/{len(LION_SHAPEFILE_URLS)}...")
                    lion_zip = downloader.download_shapefile(url, f"lion_{i}.zip")
                    lion_extract = os.path.join(downloader.temp_dir, f"lion_{i}")
                    downloader.extract_shapefile(lion_zip, lion_extract)
                    
                    lion_shp = find_shapefile_in_directory(lion_extract)
                    if lion_shp:
                        ingester.ingest_shapefile_geopandas(lion_shp, "lion_streets")
                        lion_success = True
                        break
                    else:
                        print("✗ Could not find .shp file in extracted LION archive")
                except Exception as e:
                    print(f"✗ Error with URL {i}: {e}")
                    if i < len(LION_SHAPEFILE_URLS):
                        continue
                    else:
                        print("\n⚠ All LION URLs failed. You can:")
                        print("  1. Manually download from: https://www.nyc.gov/content/planning/pages/resources/datasets/lion")
                        print("  2. Run with --lion-file /path/to/lion.zip")
        
        if not lion_success:
            print("\n⚠ LION shapefile ingestion skipped")
        
        # Download and ingest Taxi Zones shapefile
        print("\n" + "="*80)
        print("Taxi Zones Shapefile")
        print("="*80)
        print("Source: NYC Open Data - Taxi Zones")
        
        taxi_success = False
        
        # Check if local file provided
        if args.taxi_zones_file and os.path.exists(args.taxi_zones_file):
            print(f"\nUsing local Taxi Zones file: {args.taxi_zones_file}")
            try:
                if args.taxi_zones_file.endswith('.zip'):
                    taxi_extract = os.path.join(downloader.temp_dir, "taxi_zones_local")
                    downloader.extract_shapefile(args.taxi_zones_file, taxi_extract)
                    taxi_shp = find_shapefile_in_directory(taxi_extract)
                else:
                    taxi_shp = args.taxi_zones_file
                
                if taxi_shp and os.path.exists(taxi_shp):
                    ingester.ingest_shapefile_geopandas(taxi_shp, "taxi_zones")
                    taxi_success = True
                else:
                    print("✗ Could not find .shp file")
            except Exception as e:
                print(f"✗ Error processing local Taxi Zones file: {e}")
        else:
            # Try downloading from URLs
            for i, url in enumerate(TAXI_ZONES_SHAPEFILE_URLS, 1):
                try:
                    print(f"\nTrying Taxi Zones URL {i}/{len(TAXI_ZONES_SHAPEFILE_URLS)}...")
                    taxi_zip = downloader.download_shapefile(url, f"taxi_zones_{i}.zip")
                    taxi_extract = os.path.join(downloader.temp_dir, f"taxi_zones_{i}")
                    downloader.extract_shapefile(taxi_zip, taxi_extract)
                    
                    taxi_shp = find_shapefile_in_directory(taxi_extract)
                    if taxi_shp:
                        ingester.ingest_shapefile_geopandas(taxi_shp, "taxi_zones")
                        taxi_success = True
                        break
                    else:
                        print("✗ Could not find .shp file in extracted Taxi Zones archive")
                except Exception as e:
                    print(f"✗ Error with URL {i}: {e}")
                    if i < len(TAXI_ZONES_SHAPEFILE_URLS):
                        continue
                    else:
                        print("\n⚠ All Taxi Zones URLs failed. You can:")
                        print("  1. Manually download from: https://data.cityofnewyork.us/Transportation/NYC-Taxi-Zones/d3c5-ddvh")
                        print("  2. Run with --taxi-zones-file /path/to/taxi_zones.zip")
        
        if not taxi_success:
            print("\n⚠ Taxi Zones shapefile ingestion skipped")
        
        # Get collection statistics
        print("\n" + "="*80)
        print("Collection Statistics")
        print("="*80)
        
        collections = ["lion_streets", "taxi_zones"]
        for coll_name in collections:
            coll = ingester.db[coll_name]
            count = coll.count_documents({})
            if count > 0:
                print(f"\n{coll_name}:")
                print(f"  Total documents: {count:,}")
                
                # Get sample document to show structure
                sample = coll.find_one()
                if sample:
                    print(f"  Sample fields: {list(sample.keys())[:10]}")
        
        # Close connection
        ingester.close()
        
        # Cleanup
        downloader.cleanup()
        
        print("\n" + "="*80)
        print("✓ Shapefile ingestion completed!")
        print("="*80)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        downloader.cleanup()
        sys.exit(1)

if __name__ == "__main__":
    main()


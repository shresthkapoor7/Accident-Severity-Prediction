"""
NYC Motor Vehicle Collisions Data Ingestion Script
Downloads data from NYC Open Data and ingests into MongoDB
"""

import requests
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import json
from typing import Dict, List, Optional
import sys
from urllib.parse import quote

# NYC Open Data API Configuration
NYC_OPEN_DATA_BASE_URL = "https://data.cityofnewyork.us/resource"
COLLISIONS_DATASET_ID = "h9gi-nx95"  # Motor Vehicle Collisions - Crashes

# MongoDB Configuration (Docker default)
MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_DB_NAME = "nyc_collisions"
MONGO_COLLECTION_NAME = "collisions"

class NYCDataDownloader:
    """Class to handle downloading data from NYC Open Data API"""
    
    def __init__(self, dataset_id: str = COLLISIONS_DATASET_ID):
        self.dataset_id = dataset_id
        self.base_url = f"{NYC_OPEN_DATA_BASE_URL}/{dataset_id}.json"
    
    def download_data(self, limit: Optional[int] = None, offset: int = 0, 
                     date_filter: Optional[Dict[str, str]] = None) -> List[Dict]:
        """
        Download data from NYC Open Data API
        
        Args:
            limit: Maximum number of records to fetch (None for all)
            offset: Starting record number
            date_filter: Optional dict with 'start_date' and 'end_date' keys
        
        Returns:
            List of dictionaries containing collision data
        """
        params = {
            '$limit': limit if limit else 50000,  # Default limit
            '$offset': offset,
            '$order': 'crash_date DESC'
        }
        
        # Add date filter if provided
        if date_filter:
            if 'start_date' in date_filter:
                params['$where'] = f"crash_date >= '{date_filter['start_date']}'"
            if 'end_date' in date_filter:
                if '$where' in params:
                    params['$where'] += f" AND crash_date <= '{date_filter['end_date']}'"
                else:
                    params['$where'] = f"crash_date <= '{date_filter['end_date']}'"
        
        print(f"Downloading data from NYC Open Data API...")
        print(f"URL: {self.base_url}")
        print(f"Parameters: {params}")
        
        try:
            response = requests.get(self.base_url, params=params, timeout=300)
            response.raise_for_status()
            data = response.json()
            print(f"✓ Successfully downloaded {len(data)} records")
            return data
        except requests.exceptions.RequestException as e:
            print(f"✗ Error downloading data: {e}")
            raise
    
    def download_all_data(self, batch_size: int = 50000) -> List[Dict]:
        """
        Download all data in batches
        
        Args:
            batch_size: Number of records per batch
        
        Returns:
            Complete list of all collision records
        """
        all_data = []
        offset = 0
        batch_num = 1
        
        while True:
            print(f"\nDownloading batch {batch_num} (offset: {offset})...")
            batch_data = self.download_data(limit=batch_size, offset=offset)
            
            if not batch_data:
                break
            
            all_data.extend(batch_data)
            print(f"Total records so far: {len(all_data)}")
            
            # If we got fewer records than batch_size, we've reached the end
            if len(batch_data) < batch_size:
                break
            
            offset += batch_size
            batch_num += 1
        
        print(f"\n✓ Total records downloaded: {len(all_data)}")
        return all_data
    
    def get_date_range(self, data: List[Dict]) -> Dict[str, str]:
        """
        Extract date range from the data
        
        Args:
            data: List of collision records
        
        Returns:
            Dictionary with 'min_date' and 'max_date'
        """
        if not data:
            return {'min_date': None, 'max_date': None}
        
        dates = []
        for record in data:
            if 'crash_date' in record and record['crash_date']:
                try:
                    # Handle different date formats
                    date_str = record['crash_date']
                    if 'T' in date_str:
                        date_str = date_str.split('T')[0]
                    dates.append(date_str)
                except:
                    continue
        
        if not dates:
            return {'min_date': None, 'max_date': None}
        
        min_date = min(dates)
        max_date = max(dates)
        
        return {
            'min_date': min_date,
            'max_date': max_date
        }

class MongoIngester:
    """Class to handle MongoDB operations"""
    
    def __init__(self, host: str = MONGO_HOST, port: int = MONGO_PORT,
                 db_name: str = MONGO_DB_NAME, collection_name: str = MONGO_COLLECTION_NAME):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.db = None
        self.collection = None
    
    def connect(self) -> bool:
        """Connect to MongoDB"""
        try:
            print(f"\nConnecting to MongoDB at {self.host}:{self.port}...")
            self.client = MongoClient(self.host, self.port, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.server_info()
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            print(f"✓ Successfully connected to MongoDB")
            print(f"  Database: {self.db_name}")
            print(f"  Collection: {self.collection_name}")
            return True
        except Exception as e:
            print(f"✗ Error connecting to MongoDB: {e}")
            print(f"  Make sure MongoDB is running in Docker:")
            print(f"  docker run -d -p 27017:27017 --name mongodb mongo")
            return False
    
    def create_indexes(self):
        """Create indexes for better query performance"""
        try:
            print("\nCreating indexes...")
            # Index on crash_date for date range queries
            self.collection.create_index("crash_date")
            # Index on collision_id for unique lookups
            self.collection.create_index("collision_id", unique=True)
            # Index on borough for filtering
            self.collection.create_index("borough")
            # Index on location coordinates for geospatial queries
            self.collection.create_index([("latitude", 1), ("longitude", 1)])
            print("✓ Indexes created successfully")
        except Exception as e:
            print(f"⚠ Warning creating indexes: {e}")
    
    def ingest_data(self, data: List[Dict], batch_size: int = 1000, 
                   clear_existing: bool = False) -> Dict[str, int]:
        """
        Ingest data into MongoDB
        
        Args:
            data: List of records to insert
            batch_size: Number of records per batch insert
            clear_existing: Whether to clear existing data before inserting
        
        Returns:
            Dictionary with ingestion statistics
        """
        if self.collection is None:
            raise Exception("Not connected to MongoDB. Call connect() first.")
        
        stats = {
            'total_records': len(data),
            'inserted': 0,
            'updated': 0,
            'errors': 0
        }
        
        # Clear existing data if requested
        if clear_existing:
            print("\nClearing existing data...")
            result = self.collection.delete_many({})
            print(f"✓ Deleted {result.deleted_count} existing records")
        
        # Process data in batches
        print(f"\nIngesting {len(data)} records into MongoDB...")
        print(f"Batch size: {batch_size}")
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(data) + batch_size - 1) // batch_size
            
            try:
                # Prepare documents for insertion
                documents = []
                for record in batch:
                    # Clean and prepare document
                    doc = self._prepare_document(record)
                    if doc:
                        documents.append(doc)
                
                if documents:
                    # Use insert_many with ordered=False to continue on errors
                    result = self.collection.insert_many(documents, ordered=False)
                    stats['inserted'] += len(result.inserted_ids)
                    print(f"  Batch {batch_num}/{total_batches}: Inserted {len(result.inserted_ids)} records")
                
            except Exception as e:
                # Handle duplicate key errors (collision_id unique index)
                if "duplicate key" in str(e).lower() or "E11000" in str(e):
                    # Try inserting one by one to handle duplicates
                    for doc in documents:
                        try:
                            self.collection.insert_one(doc)
                            stats['inserted'] += 1
                        except Exception as dup_error:
                            if "duplicate key" in str(dup_error).lower() or "E11000" in str(dup_error):
                                # Update existing document (exclude _id)
                                update_doc = {k: v for k, v in doc.items() if k != '_id'}
                                self.collection.update_one(
                                    {"collision_id": doc.get("collision_id")},
                                    {"$set": update_doc}
                                )
                                stats['updated'] += 1
                            else:
                                stats['errors'] += 1
                else:
                    print(f"  ✗ Error in batch {batch_num}: {e}")
                    stats['errors'] += len(documents)
        
        print(f"\n✓ Ingestion complete!")
        print(f"  Total records: {stats['total_records']}")
        print(f"  Inserted: {stats['inserted']}")
        print(f"  Updated: {stats['updated']}")
        print(f"  Errors: {stats['errors']}")
        
        return stats
    
    def _prepare_document(self, record: Dict) -> Optional[Dict]:
        """Prepare a document for MongoDB insertion"""
        doc = {}
        
        for key, value in record.items():
            # Skip None values
            if value is None:
                continue
            
            # Convert numeric strings to numbers
            if isinstance(value, str):
                # Try to convert to number
                if value.replace('.', '', 1).replace('-', '', 1).isdigit():
                    try:
                        if '.' in value:
                            doc[key] = float(value)
                        else:
                            doc[key] = int(value)
                        continue
                    except:
                        pass
                
                # Handle empty strings
                if value.strip() == '':
                    continue
            
            doc[key] = value
        
        # Ensure collision_id exists (required for unique index)
        if 'collision_id' not in doc or not doc['collision_id']:
            return None
        
        return doc
    
    def get_collection_stats(self) -> Dict:
        """Get statistics about the collection"""
        if self.collection is None:
            return {}
        
        stats = {
            'total_documents': self.collection.count_documents({}),
            'date_range': self._get_date_range(),
            'boroughs': self._get_borough_counts()
        }
        
        return stats
    
    def _get_date_range(self) -> Dict[str, Optional[str]]:
        """Get min and max dates from collection"""
        try:
            pipeline = [
                {"$match": {"crash_date": {"$exists": True, "$ne": None}}},
                {"$group": {
                    "_id": None,
                    "min_date": {"$min": "$crash_date"},
                    "max_date": {"$max": "$crash_date"}
                }}
            ]
            result = list(self.collection.aggregate(pipeline))
            if result:
                return {
                    'min_date': result[0].get('min_date'),
                    'max_date': result[0].get('max_date')
                }
        except:
            pass
        return {'min_date': None, 'max_date': None}
    
    def _get_borough_counts(self) -> Dict[str, int]:
        """Get collision counts by borough"""
        try:
            pipeline = [
                {"$match": {"borough": {"$exists": True, "$ne": None}}},
                {"$group": {"_id": "$borough", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            results = list(self.collection.aggregate(pipeline))
            return {r['_id']: r['count'] for r in results}
        except:
            return {}
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            print("✓ MongoDB connection closed")

def main():
    """Main function to download and ingest data"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Download and ingest NYC Motor Vehicle Collisions data')
    parser.add_argument('--limit', type=int, default=None, 
                       help='Limit number of records to download (for testing)')
    parser.add_argument('--test', action='store_true',
                       help='Test mode: download only 1000 records')
    args = parser.parse_args()
    
    print("="*80)
    print("NYC Motor Vehicle Collisions Data Ingestion")
    print("="*80)
    
    # Initialize downloader
    downloader = NYCDataDownloader()
    
    # Download data
    print("\n" + "-"*80)
    print("STEP 1: Downloading data from NYC Open Data")
    print("-"*80)
    
    try:
        # Download data based on arguments
        if args.test:
            print("🧪 TEST MODE: Downloading 1000 records only")
            data = downloader.download_data(limit=1000)
        elif args.limit:
            print(f"📊 Downloading {args.limit} records")
            data = downloader.download_data(limit=args.limit)
        else:
            # Download all data
            print("📥 Downloading ALL data (this may take a while...)")
            data = downloader.download_all_data(batch_size=50000)
        
        if not data:
            print("✗ No data downloaded. Exiting.")
            return
        
        # Get date range
        print("\n" + "-"*80)
        print("STEP 2: Analyzing date range")
        print("-"*80)
        date_range = downloader.get_date_range(data)
        print(f"Date Range:")
        print(f"  Minimum Date: {date_range['min_date']}")
        print(f"  Maximum Date: {date_range['max_date']}")
        
        # Initialize MongoDB ingester
        ingester = MongoIngester()
        
        # Connect to MongoDB
        print("\n" + "-"*80)
        print("STEP 3: Connecting to MongoDB")
        print("-"*80)
        if not ingester.connect():
            print("✗ Failed to connect to MongoDB. Exiting.")
            return
        
        # Create indexes
        ingester.create_indexes()
        
        # Ingest data
        print("\n" + "-"*80)
        print("STEP 4: Ingesting data into MongoDB")
        print("-"*80)
        stats = ingester.ingest_data(data, batch_size=1000, clear_existing=False)
        
        # Get final statistics
        print("\n" + "-"*80)
        print("STEP 5: Collection Statistics")
        print("-"*80)
        collection_stats = ingester.get_collection_stats()
        print(f"Total documents in collection: {collection_stats.get('total_documents', 0)}")
        
        date_range_db = collection_stats.get('date_range', {})
        print(f"Date range in database:")
        print(f"  Minimum Date: {date_range_db.get('min_date')}")
        print(f"  Maximum Date: {date_range_db.get('max_date')}")
        
        borough_counts = collection_stats.get('boroughs', {})
        if borough_counts:
            print(f"\nCollisions by Borough:")
            for borough, count in borough_counts.items():
                print(f"  {borough}: {count}")
        
        # Close connection
        ingester.close()
        
        print("\n" + "="*80)
        print("✓ Data ingestion completed successfully!")
        print("="*80)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()


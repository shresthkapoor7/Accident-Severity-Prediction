# NYC Motor Vehicle Collisions Data Ingestion

This script downloads NYC Motor Vehicle Collisions data from NYC Open Data and ingests it into MongoDB.

## Prerequisites

1. **MongoDB Docker Container**: Make sure MongoDB is running in Docker
   ```bash
   docker run -d -p 27017:27017 --name mongodb mongo
   ```

2. **Python Dependencies**: Install required packages
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Basic Usage

Run the script to download all collision data and ingest into MongoDB:

```bash
python loadInMongo.py
```

### What the Script Does

1. **Downloads Data**: Fetches Motor Vehicle Collisions data from NYC Open Data API
   - Dataset ID: `h9gi-nx95`
   - Downloads in batches of 50,000 records
   - Handles pagination automatically

2. **Extracts Date Range**: Analyzes the downloaded data to find:
   - Minimum crash date
   - Maximum crash date

3. **Connects to MongoDB**: Connects to MongoDB running on:
   - Host: `localhost`
   - Port: `27017`
   - Database: `nyc_collisions`
   - Collection: `collisions`

4. **Creates Indexes**: Automatically creates indexes for:
   - `crash_date` - for date range queries
   - `collision_id` - unique index for deduplication
   - `borough` - for filtering by borough
   - `latitude`, `longitude` - for geospatial queries

5. **Ingests Data**: Inserts data in batches of 1,000 records
   - Handles duplicate records (updates existing)
   - Provides progress updates
   - Shows statistics after completion

## Configuration

You can modify these constants in the script:

```python
# MongoDB Configuration
MONGO_HOST = "localhost"
MONGO_PORT = 27017
MONGO_DB_NAME = "nyc_collisions"
MONGO_COLLECTION_NAME = "collisions"

# NYC Open Data
COLLISIONS_DATASET_ID = "h9gi-nx95"
```

## Output

The script provides:
- Date range of the data (min and max dates)
- Total records downloaded
- Ingestion statistics (inserted, updated, errors)
- Collection statistics (total documents, date range, borough counts)

## Example Output

```
================================================================================
NYC Motor Vehicle Collisions Data Ingestion
================================================================================

--------------------------------------------------------------------------------
STEP 1: Downloading data from NYC Open Data
--------------------------------------------------------------------------------
Downloading data from NYC Open Data API...
✓ Successfully downloaded 50000 records

Downloading batch 2 (offset: 50000)...
✓ Successfully downloaded 50000 records

✓ Total records downloaded: 150000

--------------------------------------------------------------------------------
STEP 2: Analyzing date range
--------------------------------------------------------------------------------
Date Range:
  Minimum Date: 2012-07-01
  Maximum Date: 2024-11-29

--------------------------------------------------------------------------------
STEP 3: Connecting to MongoDB
--------------------------------------------------------------------------------
Connecting to MongoDB at localhost:27017...
✓ Successfully connected to MongoDB
  Database: nyc_collisions
  Collection: collisions

Creating indexes...
✓ Indexes created successfully

--------------------------------------------------------------------------------
STEP 4: Ingesting data into MongoDB
--------------------------------------------------------------------------------
Ingesting 150000 records into MongoDB...
  Batch 1/150: Inserted 1000 records
  ...
✓ Ingestion complete!
  Total records: 150000
  Inserted: 150000
  Updated: 0
  Errors: 0

--------------------------------------------------------------------------------
STEP 5: Collection Statistics
--------------------------------------------------------------------------------
Total documents in collection: 150000
Date range in database:
  Minimum Date: 2012-07-01
  Maximum Date: 2024-11-29

Collisions by Borough:
  BROOKLYN: 45000
  QUEENS: 35000
  MANHATTAN: 30000
  BRONX: 25000
  STATEN ISLAND: 15000
```

## Troubleshooting

### MongoDB Connection Error

If you get a connection error:
1. Check if MongoDB Docker container is running:
   ```bash
   docker ps | grep mongodb
   ```

2. Start MongoDB if not running:
   ```bash
   docker start mongodb
   ```

3. Or create a new container:
   ```bash
   docker run -d -p 27017:27017 --name mongodb mongo
   ```

### API Rate Limiting

If you encounter rate limiting from NYC Open Data API:
- The script handles pagination automatically
- You can reduce batch size in `download_all_data(batch_size=...)`
- Add delays between requests if needed

### Memory Issues

For very large datasets:
- Process data in smaller batches
- Use `limit` parameter in `download_data()` for testing
- Consider using MongoDB's bulk operations for better performance

## Data Source

- **NYC Open Data**: https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95
- **API Documentation**: https://dev.socrata.com/foundry/data.cityofnewyork.us/h9gi-nx95


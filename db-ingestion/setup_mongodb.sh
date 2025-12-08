#!/bin/bash

# Setup MongoDB Docker Container for NYC Collisions Data Ingestion

echo "Setting up MongoDB Docker container..."

# Check if MongoDB container already exists
if docker ps -a | grep -q mongodb; then
    echo "MongoDB container exists. Stopping and removing..."
    docker stop mongodb 2>/dev/null
    docker rm mongodb 2>/dev/null
fi

# Start MongoDB container with port mapping
echo "Starting MongoDB container with port 27017 mapped to host..."
docker run -d \
    --name mongodb \
    -p 27017:27017 \
    -v mongodb_data:/data/db \
    mongo:latest

# Wait for MongoDB to be ready
echo "Waiting for MongoDB to be ready..."
sleep 5

# Test connection
if docker exec mongodb mongosh --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
    echo "✓ MongoDB is running and accessible on localhost:27017"
    echo ""
    echo "You can now run: python loadInMongo.py"
else
    echo "⚠ MongoDB container started but may need more time to initialize"
    echo "Wait a few seconds and try running: python loadInMongo.py"
fi


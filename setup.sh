#!/bin/bash

# Fuel Exports ETL Setup Script
# This script sets up the entire Airflow ETL pipeline

set -e

# Add common Docker paths to PATH for this script
export PATH="/usr/local/bin:$PATH"

echo "🚀 Setting up Fuel Exports ETL Pipeline..."

# Function to find Docker command
find_docker() {
    if command -v docker &> /dev/null; then
        echo "docker"
    elif [ -f "/usr/local/bin/docker" ]; then
        echo "/usr/local/bin/docker"
    elif [ -f "/Applications/Docker.app/Contents/Resources/bin/docker" ]; then
        echo "/Applications/Docker.app/Contents/Resources/bin/docker"
    else
        return 1
    fi
}

# Function to find Docker Compose command
find_docker_compose() {
    if command -v docker-compose &> /dev/null; then
        echo "docker-compose"
    elif [ -f "/usr/local/bin/docker-compose" ]; then
        echo "/usr/local/bin/docker-compose"
    elif command -v docker &> /dev/null && docker compose version &> /dev/null; then
        echo "docker compose"
    elif [ -f "/usr/local/bin/docker" ] && /usr/local/bin/docker compose version &> /dev/null; then
        echo "/usr/local/bin/docker compose"
    else
        return 1
    fi
}

# Check if Docker is available
DOCKER_CMD=$(find_docker)
if [ $? -ne 0 ]; then
    echo "❌ Docker is not installed or not found. Please install Docker Desktop first."
    echo "   Download from: https://www.docker.com/products/docker-desktop"
    exit 1
fi

# Check if Docker Compose is available
DOCKER_COMPOSE_CMD=$(find_docker_compose)
if [ $? -ne 0 ]; then
    echo "❌ Docker Compose is not available. Please make sure Docker Desktop is installed and running."
    exit 1
fi

echo "✅ Found Docker: $DOCKER_CMD"
echo "✅ Found Docker Compose: $DOCKER_COMPOSE_CMD"

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p data logs

# Copy environment template if .env doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating environment configuration..."
    cp config/.env.template .env
    echo "✅ Created .env file from template. Please review and modify if needed."
fi

# Set proper permissions for Airflow
echo "🔧 Setting up permissions..."
sudo chown -R 50000:0 logs/
sudo chown -R 50000:0 data/

# Initialize Airflow
echo "🏗️  Initializing Airflow..."
$DOCKER_COMPOSE_CMD up airflow-init

# Start services
echo "🚀 Starting services..."
$DOCKER_COMPOSE_CMD up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 30

# Check if services are healthy
echo "🔍 Checking service health..."
$DOCKER_COMPOSE_CMD ps

echo "✅ Setup complete!"
echo ""
echo "🌐 Airflow Web UI: http://localhost:8080"
echo "   Username: admin"
echo "   Password: admin"
echo ""
echo "🐘 PostgreSQL: localhost:5432"
echo "   Database: airflow"
echo "   Username: airflow" 
echo "   Password: airflow"
echo ""
echo "📊 To start generating data:"
echo "   pip install faker pyarrow pandas"
echo "   python generate_fuel_exports.py --rows-per-file 300 --period-seconds 60"
echo ""
echo "🎯 Next steps:"
echo "   1. Open Airflow UI and enable the 'fuel_exports_etl' DAG"
echo "   2. Start the data generator script"
echo "   3. Monitor the pipeline in Airflow UI"
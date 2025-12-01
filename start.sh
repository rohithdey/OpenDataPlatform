#!/bin/bash

# Open Data Platform - Startup Script
# This script initializes and starts all platform components

set -e

echo "=========================================="
echo "  Open Data Platform - Starting Up"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running. Please start Docker first.${NC}"
    exit 1
fi

# Create necessary directories
echo -e "${YELLOW}Creating directories...${NC}"
mkdir -p ./data ./airflow/logs ./airflow/plugins

# Set correct permissions for Airflow
echo -e "${YELLOW}Setting permissions...${NC}"
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux
    echo "AIRFLOW_UID=$(id -u)" > .env.local
    cat .env >> .env.local
    mv .env.local .env
fi

# Build and start services
echo -e "${YELLOW}Building and starting services...${NC}"
docker compose up --build -d

# Wait for services to be healthy
echo -e "${YELLOW}Waiting for services to start...${NC}"
echo "This may take a few minutes on first run..."

# Wait for Airflow webserver
echo -n "Waiting for Airflow"
until curl -s http://localhost:8080/health > /dev/null 2>&1; do
    echo -n "."
    sleep 5
done
echo -e " ${GREEN}Ready!${NC}"

# Wait for API
echo -n "Waiting for API"
until curl -s http://localhost:8000/health > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo -e " ${GREEN}Ready!${NC}"

# Wait for UI
echo -n "Waiting for UI"
until curl -s http://localhost:3000 > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo -e " ${GREEN}Ready!${NC}"

echo ""
echo -e "${GREEN}=========================================="
echo "  Platform is ready!"
echo "==========================================${NC}"
echo ""
echo "Access the following services:"
echo ""
echo -e "  ${GREEN}UI Dashboard:${NC}     http://localhost:3000"
echo -e "  ${GREEN}Airflow:${NC}          http://localhost:8080"
echo -e "  ${GREEN}API:${NC}              http://localhost:8000"
echo -e "  ${GREEN}API Docs:${NC}         http://localhost:8000/docs"
echo ""
echo "Default Airflow credentials: admin / admin"
echo ""
echo "To stop the platform, run: docker compose down"
echo ""

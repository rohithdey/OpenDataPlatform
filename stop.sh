#!/bin/bash

# Open Data Platform - Stop Script

echo "Stopping Open Data Platform..."
docker compose down

echo "Platform stopped."
echo ""
echo "To remove all data volumes as well, run:"
echo "  docker compose down -v"

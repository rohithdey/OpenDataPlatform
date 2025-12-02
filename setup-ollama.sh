#!/bin/bash

# Setup script for Ollama with SQLCoder model
# This script pulls the SQLCoder model into Ollama on first run

echo "🚀 Setting up Ollama with SQLCoder for local AI..."
echo ""

# Check if Ollama container is running
if ! docker ps | grep -q ollama; then
    echo "❌ Ollama container is not running. Please start it first with:"
    echo "   docker-compose up -d ollama"
    exit 1
fi

echo "✅ Ollama container is running"
echo ""

# Wait for Ollama to be ready
echo "⏳ Waiting for Ollama to be ready..."
for i in {1..30}; do
    if curl -s http://localhost:11434/api/tags > /dev/null 2>&1; then
        echo "✅ Ollama is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Ollama failed to start after 30 seconds"
        exit 1
    fi
    sleep 1
done
echo ""

# Pull SQLCoder model
echo "📥 Pulling SQLCoder model (this may take a few minutes on first run)..."
echo "   Model size: ~4GB"
echo ""

docker exec ollama ollama pull sqlcoder

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ SQLCoder model installed successfully!"
    echo ""
    echo "🎉 Setup complete! Your platform now has:"
    echo "   • Local AI (Ollama + SQLCoder)"
    echo "   • Zero API costs per query"
    echo "   • Complete data privacy (everything stays local)"
    echo "   • Perfect for on-prem deployments"
    echo ""
    echo "The platform will automatically use Ollama for natural language queries."
    echo "OpenAI will be used as fallback if configured."
else
    echo ""
    echo "❌ Failed to pull SQLCoder model"
    echo "   You can try manually with: docker exec ollama ollama pull sqlcoder"
    exit 1
fi

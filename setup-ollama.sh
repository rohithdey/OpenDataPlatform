#!/bin/bash

# Setup script for Ollama with SQLCoder and Llama 3.2 models
# This script pulls both models for dual-model AI (SQL generation + natural language answers)

echo "🚀 Setting up Ollama with SQLCoder + Llama 3.2 for local AI..."
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

# Pull SQLCoder model (for SQL generation)
echo "📥 Pulling SQLCoder model (for SQL generation)..."
echo "   Model size: ~4GB"
echo ""

docker exec ollama ollama pull sqlcoder

if [ $? -ne 0 ]; then
    echo ""
    echo "❌ Failed to pull SQLCoder model"
    echo "   You can try manually with: docker exec ollama ollama pull sqlcoder"
    exit 1
fi

echo ""
echo "✅ SQLCoder model installed successfully!"
echo ""

# Pull Llama 3.2 model (for natural language answers)
echo "📥 Pulling Llama 3.2 model (for natural language answers)..."
echo "   Model size: ~2GB"
echo ""

docker exec ollama ollama pull llama3.2:3b

if [ $? -ne 0 ]; then
    echo ""
    echo "❌ Failed to pull Llama 3.2 model"
    echo "   You can try manually with: docker exec ollama ollama pull llama3.2:3b"
    exit 1
fi

echo ""
echo "✅ Llama 3.2 model installed successfully!"
echo ""

echo "🎉 Setup complete! Your platform now has:"
echo "   • Local AI (Ollama + SQLCoder + Llama 3.2)"
echo "   • SQLCoder: Specialized for SQL query generation"
echo "   • Llama 3.2: Natural language answers"
echo "   • Zero API costs per query"
echo "   • Complete data privacy (everything stays local)"
echo "   • Perfect for on-prem deployments"
echo ""
echo "The platform will automatically use:"
echo "   1. SQLCoder for SQL generation"
echo "   2. Llama 3.2 for natural language answers"
echo "   3. OpenAI as fallback if configured"

# 🍎 macOS Setup Guide (M1/M2/M3/M4)

This guide is specifically for **macOS with Apple Silicon** (M1/M2/M3/M4). If you're on Linux or Windows, use the standard [DEPLOYMENT.md](DEPLOYMENT.md) instead.

## Why Native Ollama on macOS?

**Docker on Mac = Linux VM = No GPU access = Slow**
- Runs on CPU only
- 10-60 seconds per query
- 100% CPU usage

**Native Ollama on macOS = Metal GPU = Fast**
- Uses Neural Engine acceleration
- 1-6 seconds per query (5-10x faster!)
- 20-40% CPU usage
- Better memory efficiency with unified memory

## 🚀 Quick Start

### 1. Install Native Ollama

```bash
# Make the setup script executable
chmod +x setup-ollama-mac.sh

# Run the setup (installs Ollama + pulls models)
./setup-ollama-mac.sh
```

This will:
- Install Ollama via Homebrew (if not already installed)
- Start Ollama service
- Pull SQLCoder model (~4GB)
- Pull Llama 3.2 model (~2GB)
- Configure for Metal GPU acceleration

**First run takes 5-10 minutes to download models. After that, queries are instant!**

### 2. Start Docker Containers (Using Native Ollama)

```bash
# Use the Mac-specific compose file
docker-compose -f docker-compose.yml -f docker-compose.mac.yml up -d --build
```

Or set environment variable (recommended):

```bash
# Add to your ~/.zshrc or ~/.bash_profile
export COMPOSE_FILE=docker-compose.yml:docker-compose.mac.yml

# Then just use normal commands
docker-compose up -d --build
docker-compose down
docker-compose logs -f
```

### 3. Verify It's Working

1. Check Ollama is running natively:
   ```bash
   brew services list | grep ollama
   # Should show: ollama started

   ollama list
   # Should show: sqlcoder and llama3.2:3b
   ```

2. Go to http://localhost:3000

3. Click "Ask Data" tab

4. Type a question: "Show me all tables"

5. You should see:
   - Response in **1-6 seconds** (vs 10-60s with Docker)
   - Green badge showing "🚀 Ollama (SQLCoder)"
   - Much lower CPU usage in Activity Monitor

## 📊 Performance Comparison on M4 Mac (16GB)

| Setup | Query Time | CPU Usage | GPU/Neural Engine |
|-------|------------|-----------|-------------------|
| Docker Ollama | 10-60s | 100% | ❌ No access |
| **Native Ollama** | **1-6s** | **20-40%** | ✅ Metal GPU |

## 🔧 Management Commands

### Ollama Service

```bash
# Check status
brew services list | grep ollama

# Start Ollama
brew services start ollama

# Stop Ollama
brew services stop ollama

# Restart Ollama
brew services restart ollama

# View Ollama logs
tail -f $(brew --prefix)/var/log/ollama.log
```

### Models

```bash
# List installed models
ollama list

# Pull a new model
ollama pull llama3.2:3b

# Remove a model
ollama rm llama3.2:3b

# Test a model directly
ollama run llama3.2:3b "Hello, how are you?"
```

### Docker Containers

```bash
# View logs
docker-compose logs -f api

# Check API is connecting to native Ollama
docker-compose logs api | grep "Using Ollama"

# Restart just the API
docker-compose restart api

# Rebuild after code changes
docker-compose up -d --build api
```

## 🛠️ Troubleshooting

### "Connection refused" or Ollama not responding

```bash
# Check if Ollama is running
brew services list | grep ollama

# If not running, start it
brew services start ollama

# Wait a few seconds, then test
curl http://localhost:11434/api/tags

# Restart Docker containers
docker-compose restart api
```

### Models not found

```bash
# List models
ollama list

# Pull models manually
ollama pull sqlcoder
ollama pull llama3.2:3b
```

### Still seeing slow responses

1. Check Activity Monitor → Make sure "ollama" process is using GPU
2. Verify you're using the Mac compose file:
   ```bash
   docker-compose config | grep OLLAMA_BASE_URL
   # Should show: http://host.docker.internal:11434
   ```
3. Check API logs:
   ```bash
   docker-compose logs api | tail -20
   ```

### Docker can't connect to native Ollama

This means `host.docker.internal` isn't working. Try:

```bash
# Get your Mac's IP address
ipconfig getifaddr en0

# Update docker-compose.mac.yml to use your IP instead
# Change: http://host.docker.internal:11434
# To: http://192.168.1.XXX:11434
```

## 🔄 Switching Back to Docker Ollama

If you want to switch back to Docker-based Ollama:

```bash
# Stop native Ollama
brew services stop ollama

# Use standard docker-compose
docker-compose -f docker-compose.yml up -d
```

## 💰 Cost Comparison

| Solution | Setup | Query Speed (M4) | CPU Usage | Cost per Query |
|----------|-------|------------------|-----------|----------------|
| **Native Ollama** | **5 min** | **1-6s** | **20-40%** | **$0** |
| Docker Ollama | 10 min | 10-60s | 100% | $0 |
| OpenAI GPT-4 | 1 min | 2-5s | 0% | $0.10 |

## 📦 System Requirements

### Minimum (Development)
- Mac with Apple Silicon (M1/M2/M3/M4)
- macOS 12.0 or later
- 8GB unified memory
- 25GB disk space

### Recommended (Production)
- Mac with M2 Pro/Max or M3/M4
- 16GB+ unified memory
- 50GB disk space

## 🎯 Performance Tips

1. **Keep models loaded**: First query after restart is slower (model loading)
2. **Close other apps**: More memory = faster inference
3. **Use smaller models**: `llama3.2:1b` is 3x faster than `llama3.2:3b`
4. **Monitor memory**: Check Activity Monitor → Memory tab

## 🚀 Next Steps

Once everything is working:
1. ✅ Native Ollama is 5-10x faster on M4
2. ✅ Lower CPU usage (GPU acceleration)
3. ✅ Zero API costs
4. ✅ Complete data privacy (everything local)
5. ✅ Perfect for on-prem deployments

Ready to deploy to customers? See [DEPLOYMENT.md](DEPLOYMENT.md) for production setup.

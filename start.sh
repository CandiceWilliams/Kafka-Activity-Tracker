#!/bin/bash
set -e

echo "🚀 Starting Kafka Activity Tracker..."

echo "📦 Starting Docker services..."
docker-compose up -d

echo "⏳ Waiting for services to be ready..."
sleep 30

echo "📝 Creating Kafka topics..."
python scripts/setup_topics.py

echo "🔧 Setting up ksqlDB..."
python scripts/setup_ksqldb.py

echo "✅ Setup complete!"
echo "🌐 Starting FastAPI..."
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
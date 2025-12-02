# 📘 Kafka Activity Tracker - Complete Setup Guide

This guide provides detailed, step-by-step instructions for setting up and running the Kafka Activity Tracker project.

---

## 📋 Table of Contents

1. [Prerequisites](#prerequisites)
2. [Initial Setup](#initial-setup)
3. [Starting the Kafka Cluster](#starting-the-kafka-cluster)
4. [Setting Up the Python Environment](#setting-up-the-python-environment)
5. [Creating Kafka Topics](#creating-kafka-topics)
6. [Running the Application](#running-the-application)
7. [Verifying the Setup](#verifying-the-setup)
8. [Development Workflow](#development-workflow)
9. [Troubleshooting](#troubleshooting)
10. [Advanced Configuration](#advanced-configuration)

---

## Prerequisites

### Required Software

Before starting, ensure you have the following installed on your system:

#### 1. Docker Desktop

**Download:**

- Windows/Mac: https://www.docker.com/products/docker-desktop
- Linux: Follow distribution-specific instructions

**Minimum Requirements:**

- Docker Engine: 20.10+
- Docker Compose: 2.0+
- RAM: 8GB (4GB minimum, but Kafka cluster needs resources)
- Disk Space: 10GB free

**Verify Installation:**

```bash
docker --version
# Should output: Docker version 20.10.x or higher

docker-compose --version
# Should output: Docker Compose version 2.x.x or higher
```

**Mac/Linux Users:** Ensure Docker daemon is running:

```bash
docker ps
# Should show an empty list or running containers (not an error)
```

#### 2. Python

**Download:** https://www.python.org/downloads/

**Required Version:** Python 3.9 or higher

**Verify Installation:**

```bash
python --version
# or on some systems:
python3 --version

# Should output: Python 3.9.x or higher
```

**Note for Windows users:** Make sure "Add Python to PATH" was checked during installation.

#### 3. pip (Python Package Manager)

Usually comes with Python, but verify:

```bash
pip --version
# or
pip3 --version

# Should output: pip 21.x.x or higher
```

#### 4. Git

**Download:** https://git-scm.com/downloads

**Verify Installation:**

```bash
git --version
# Should output: git version 2.x.x or higher
```

### Optional but Recommended

- **Visual Studio Code** with Python extension
- **Postman** or **Insomnia** for API testing
- **Terminal/Command Prompt** with elevated privileges

---

## Initial Setup

### Step 1: Clone the Repository

```bash
# Navigate to where you want the project
cd ~/Documents  # or C:\Users\YourName\Documents on Windows

# Clone the repository
git clone https://github.com/your-username/kafka-activity-tracker.git

# Enter the project directory
cd kafka-activity-tracker

# Verify you're in the right place
ls  # or 'dir' on Windows
# Should see: app/, docker-compose.yml, requirements.txt, etc.
```

### Step 2: Create a Virtual Environment

**Why use a virtual environment?**

- Isolates project dependencies
- Prevents conflicts with system Python packages
- Makes dependency management easier

**On Mac/Linux:**

```bash
# Create virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate

# Your prompt should now show (venv)
```

**On Windows:**

```bash
# Create virtual environment
python -m venv venv

# Activate it
venv\Scripts\activate

# Your prompt should now show (venv)
```

**Verify Activation:**

```bash
which python  # Mac/Linux
# Should point to: /path/to/kafka-activity-tracker/venv/bin/python

where python  # Windows
# Should point to: C:\path\to\kafka-activity-tracker\venv\Scripts\python.exe
```

**Deactivating Later:**

```bash
deactivate  # Works on all platforms
```

---

## Starting the Kafka Cluster

### Step 3: Configure Docker (First-Time Setup)

**Check Docker Resources:**

1. Open Docker Desktop
2. Go to Settings > Resources
3. Ensure:
   - CPUs: At least 4 (if available)
   - Memory: At least 6GB (8GB recommended)
   - Swap: 2GB
   - Disk: 10GB free space

**Why these resources?**

- 3 Kafka brokers are resource-intensive
- Insufficient resources will cause brokers to crash

### Step 4: Review docker-compose.yml

Before starting, take a moment to understand the configuration:

```bash
# View the docker-compose file
cat docker-compose.yml
```

**Key Services:**

- `kafka-1`, `kafka-2`, `kafka-3`: Three Kafka brokers for fault tolerance
- `kafka-ui`: Web interface for monitoring Kafka
- Ports exposed:
  - 9092, 9093, 9094: Kafka brokers
  - 8080: Kafka UI
  - 8000: FastAPI application (when we run it)

### Step 5: Start the Kafka Cluster

```bash
# Start all services in detached mode
docker-compose up -d

# This will:
# 1. Download Kafka images (first time only - may take 5-10 minutes)
# 2. Create network
# 3. Start 3 Kafka brokers
# 4. Start Kafka UI
```

**Expected Output:**

```
Creating network "kafka-activity-tracker_kafka-network" ... done
Creating kafka-1 ... done
Creating kafka-2 ... done
Creating kafka-3 ... done
Creating kafka-ui ... done
```

### Step 6: Verify Kafka Cluster is Running

**Check Container Status:**

```bash
docker-compose ps

# Should show 4 containers running:
# kafka-1, kafka-2, kafka-3, kafka-ui
# All with status "Up" or "healthy"
```

**View Logs (if issues):**

```bash
# View logs for all services
docker-compose logs

# View logs for specific service
docker-compose logs kafka-1
docker-compose logs kafka-ui

# Follow logs in real-time
docker-compose logs -f kafka-1
```

**Access Kafka UI:**

1. Open browser: http://localhost:8080
2. You should see:
   - Cluster: "local-cluster"
   - Brokers: 3 brokers listed (ID 1, 2, 3)
   - Topics: Empty (we'll create them next)

**If Kafka UI doesn't load:**

- Wait 30-60 seconds (Kafka takes time to start)
- Check logs: `docker-compose logs kafka-ui`
- Ensure no port conflicts on 8080

---

## Setting Up the Python Environment

### Step 7: Install Python Dependencies

**Ensure virtual environment is activated** (you should see `(venv)` in prompt)

```bash
# Upgrade pip first
pip install --upgrade pip

# Install project dependencies
pip install -r requirements.txt

# This installs:
# - fastapi (web framework)
# - uvicorn (web server)
# - kafka-python (Kafka client)
# - pydantic (data validation)
# - jinja2 (templating)
# - aiofiles (async file operations)
```

**Verify Installation:**

```bash
pip list

# Should see all packages listed, including:
# fastapi, kafka-python, pydantic, etc.
```

**If Installation Fails:**

Common issues:

```bash
# Issue: Permission denied
# Solution: Don't use sudo, ensure venv is activated

# Issue: pip not found
# Solution: Use python -m pip instead
python -m pip install -r requirements.txt

# Issue: Version conflicts
# Solution: Upgrade pip and try again
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

---

## Creating Kafka Topics

### Step 8: Create Configuration File

**Create `.env` file** in project root:

```bash
# Create .env file
touch .env  # Mac/Linux
# or
echo. > .env  # Windows

# Edit with your text editor and add:
```

**.env contents:**

```env
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093,localhost:9094
REPLICATION_FACTOR=3
PARTITIONS_HIGH_VOLUME=3
PARTITIONS_MEDIUM_VOLUME=2

# Application Configuration
APP_HOST=0.0.0.0
APP_PORT=8000
LOG_LEVEL=INFO

# Consumer Groups
CONSUMER_GROUP_ANALYTICS=analytics-processor-group
CONSUMER_GROUP_DASHBOARD=dashboard-consumer-group
```

**Why these settings?**

- **3 bootstrap servers**: Connect to any broker; Kafka handles the rest
- **Replication factor 3**: Each message copied to all 3 brokers (durability)
- **Partition counts**: More partitions for high-volume topics (parallelism)

### Step 9: Initialize Kafka Topics

```bash
# Run topic creation script
python scripts/setup_topics.py
```

**Expected Output:**

```
Creating Kafka topics...
Created 8 topics successfully:
  - page-views (3 partitions, replication: 3)
  - button-clicks (3 partitions, replication: 3)
  - slider-events (2 partitions, replication: 3)
  - dropdown-selections (2 partitions, replication: 3)
  - text-inputs (2 partitions, replication: 3)
  - toggle-events (2 partitions, replication: 3)
  - user-sessions (2 partitions, replication: 3)
  - analytics-results (1 partition, replication: 3)
Done!
```

**Verify in Kafka UI:**

1. Refresh http://localhost:8080
2. Click "Topics" in sidebar
3. Should see all 8 topics listed
4. Click on any topic to see:
   - Partition count
   - Replication factor
   - Leader distribution

**If Topics Aren't Created:**

```bash
# Check if Kafka is accessible
docker-compose ps  # All should be "healthy"

# Check Python can connect to Kafka
python -c "from kafka import KafkaAdminClient; print('Connection test'); client = KafkaAdminClient(bootstrap_servers='localhost:9092'); print('Success!')"

# If connection fails, check:
# 1. Firewall blocking ports 9092-9094
# 2. Kafka logs: docker-compose logs kafka-1
# 3. Wait longer for Kafka to fully start (can take 1-2 minutes)
```

---

## Running the Application

### Step 10: Start the FastAPI Application

```bash
# From project root, with venv activated
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**Flags explained:**

- `app.main:app`: Run the `app` object from `app/main.py`
- `--reload`: Auto-restart when code changes (development mode)
- `--host 0.0.0.0`: Listen on all network interfaces
- `--port 8000`: Use port 8000

**Expected Output:**

```
INFO:     Will watch for changes in these directories: ['/path/to/kafka-activity-tracker']
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [12345] using StatReload
INFO:     Started server process [12346]
INFO:     Waiting for application startup.
Kafka Producer connected
Kafka Consumer connected
INFO:     Application startup complete.
```

**What's Happening:**

1. FastAPI starts web server
2. Kafka producer connects to brokers
3. Consumer threads start in background
4. Application ready to receive events

### Step 11: Test Basic Functionality

**Open Browser:**

1. **Activity Tracker**: http://localhost:8000

   - You should see the activity tracking page
   - Try clicking buttons, moving slider, etc.

2. **Dashboard**: http://localhost:8000/dashboard

   - Should show analytics (initially all zeros)
   - Updates every second

3. **API Documentation**: http://localhost:8000/docs
   - FastAPI's automatic API documentation
   - Can test endpoints here

**Test an Event:**

Click a button on the tracker page. Then:

1. Check terminal logs (should see "Event received")
2. Check dashboard (button click count should increment)
3. Check Kafka UI → Topics → button-clicks → Messages tab
   - Should see your event in JSON format

---

## Verifying the Setup

### Step 12: End-to-End Verification

Run through this checklist to ensure everything works:

#### Kafka Cluster Health

```bash
# Check all brokers are running
docker-compose ps
# ✓ All 4 containers show "Up" or "healthy"

# Check Kafka UI
# ✓ Open http://localhost:8080
# ✓ See 3 brokers in cluster
# ✓ All brokers show as "ONLINE"
```

#### Topics Created

```bash
# In Kafka UI:
# ✓ Navigate to Topics
# ✓ See all 8 topics listed
# ✓ Each topic shows correct partition count
# ✓ Each topic shows replication factor of 3
```

#### Application Running

```bash
# Check application logs
# ✓ No errors in terminal
# ✓ See "Kafka Producer connected"
# ✓ See "Kafka Consumer connected"

# Check endpoints
# ✓ http://localhost:8000 loads
# ✓ http://localhost:8000/dashboard loads
# ✓ http://localhost:8000/docs loads
```

#### Event Flow

```bash
# 1. Click button on tracker page
# ✓ No errors in browser console (F12)

# 2. Check terminal logs
# ✓ See "Event received" message

# 3. Check dashboard
# ✓ Button click count increments

# 4. Check Kafka UI
# ✓ Navigate to button-clicks topic
# ✓ See message in Messages tab
# ✓ Message has correct JSON structure
```

#### Consumer Groups

```bash
# In Kafka UI:
# ✓ Navigate to Consumers
# ✓ See "analytics-processor-group"
# ✓ Consumer shows 0 lag (keeping up)
# ✓ See active members in group
```

**If ANY of these checks fail, see [Troubleshooting](#troubleshooting) section below.**

---

## Development Workflow

### Daily Workflow

**Starting Work:**

```bash
# 1. Ensure Docker is running
docker-compose ps  # Check containers

# 2. If containers stopped, restart
docker-compose up -d

# 3. Activate virtual environment
source venv/bin/activate  # Mac/Linux
# or
venv\Scripts\activate  # Windows

# 4. Start application
uvicorn app.main:app --reload
```

**During Development:**

- Code changes auto-reload (thanks to `--reload` flag)
- Check logs in terminal for errors
- Use Kafka UI to monitor messages
- Use `/docs` endpoint to test API

**Ending Work:**

```bash
# 1. Stop application (Ctrl+C in terminal)

# 2. Stop Kafka (optional - can leave running)
docker-compose down

# 3. Deactivate virtual environment
deactivate
```

### Making Changes

**Adding a New Event Type:**

1. Add model to `app/models/events.py`
2. Add topic name to `app/config.py`
3. Update `TOPIC_MAPPING` in `app/config.py`
4. Add endpoint to `app/main.py`
5. Update `scripts/setup_topics.py`
6. Recreate topics: `python scripts/setup_topics.py`
7. Test with new endpoint

**Modifying Topic Configuration:**

1. Stop application
2. Delete topics in Kafka UI (Topics → Select → Delete)
3. Modify `app/config.py`
4. Run `python scripts/setup_topics.py`
5. Restart application

**Changing Consumer Logic:**

1. Modify `app/kafka/consumer.py` or `app/services/analytics.py`
2. Application auto-reloads (if using `--reload`)
3. Check logs to ensure no errors
4. Test by generating events

---

## Troubleshooting

### Kafka Won't Start

**Symptoms:**

- Docker containers exit immediately
- `docker-compose ps` shows containers as "Exit 1"

**Solutions:**

**Check Ports:**

```bash
# Check if ports are already in use
# Mac/Linux:
lsof -i :9092
lsof -i :8080

# Windows:
netstat -ano | findstr :9092
netstat -ano | findstr :8080

# If ports are in use:
# Option 1: Kill the process using the port
# Option 2: Change ports in docker-compose.yml
```

**Check Docker Resources:**

```bash
# Ensure Docker has enough memory
docker system info | grep Memory

# Should show at least 6GB available
# If not, increase in Docker Desktop settings
```

**Check Logs:**

```bash
docker-compose logs kafka-1
# Look for errors like:
# - "Out of memory"
# - "Port already in use"
# - "Unable to bind"
```

**Nuclear Option (Reset Everything):**

```bash
# Stop and remove everything
docker-compose down -v

# Remove images
docker-compose down --rmi all

# Restart
docker-compose up -d
```

### Producer Can't Connect

**Symptoms:**

- Application starts but shows "Kafka not ready... retrying"
- Events sent but don't appear in Kafka UI

**Solutions:**

```bash
# 1. Verify Kafka is healthy
docker-compose ps
# All brokers should be "healthy"

# 2. Test connection manually
python -c "from kafka import KafkaProducer; p = KafkaProducer(bootstrap_servers='localhost:9092'); print('Connected!')"

# 3. Check bootstrap servers in .env
cat .env
# Should show: KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093,localhost:9094

# 4. Try connecting to individual brokers
telnet localhost 9092  # Should connect
telnet localhost 9093
telnet localhost 9094
```

**If running in Docker:**

```bash
# Use internal hostnames instead
KAFKA_BOOTSTRAP_SERVERS=kafka-1:29092,kafka-2:29092,kafka-3:29092
```

### Consumer Not Receiving Messages

**Symptoms:**

- Messages appear in Kafka UI
- Dashboard doesn't update
- No "Processing message" logs

**Solutions:**

```bash
# 1. Check consumer group in Kafka UI
# Navigate to: Consumers → analytics-processor-group
# Should show:
# - Active members
# - Assigned partitions
# - Lag (should be 0 or low)

# 2. Check consumer logs
# Look in application terminal for:
# - "Kafka Consumer connected"
# - "Processing message from topic=..."

# 3. Verify consumer configuration
# In app/kafka/consumer.py, check:
# - Topic names match config
# - auto_offset_reset is set correctly
# - Consumer group ID is unique
```

**Reset Consumer Offsets (Last Resort):**

```bash
# In Kafka UI:
# Consumers → [group name] → Reset Offsets
# This makes consumer re-read all messages
```

### High CPU/Memory Usage

**Symptoms:**

- Computer slows down
- Docker consuming excessive resources

**Solutions:**

**Reduce Resource Usage:**

```bash
# Stop unused services
docker-compose down

# Run with just 1 broker (development only)
docker-compose up kafka-1 kafka-ui -d

# Adjust settings in docker-compose.yml:
# - Reduce KAFKA_LOG_RETENTION_HOURS
# - Reduce KAFKA_HEAP_OPTS memory settings
```

**Optimize Application:**

```python
# In app/kafka/producer.py, increase batching:
batch_size=32768  # Increase from 16384
linger_ms=50      # Increase from 10

# In app/kafka/consumer.py, increase fetch size:
max_poll_records=1000  # Increase from 500
```

### Messages Out of Order

**Symptoms:**

- Events processed in wrong sequence
- Analytics show incorrect counts

**Root Cause:**

- Events with different keys go to different partitions
- Different partitions process independently

**Solutions:**

```python
# Ensure events with same session go to same partition
# In app/kafka/producer.py:
def send_event(self, event: BaseEvent):
    key = event.session_id  # Use session ID as key
    # This ensures ordering within a session
```

**Verify Partition Assignment:**

```bash
# In Kafka UI:
# Topics → [topic name] → Messages
# Check "Partition" column
# Events with same key should have same partition
```

### Application Won't Start

**Symptoms:**

- `uvicorn app.main:app` shows errors
- Import errors or module not found

**Solutions:**

**Check Virtual Environment:**

```bash
# Ensure venv is activated
which python  # Should point to venv

# If not activated:
source venv/bin/activate  # Mac/Linux
venv\Scripts\activate  # Windows
```

**Reinstall Dependencies:**

```bash
pip install --upgrade pip
pip install -r requirements.txt --force-reinstall
```

**Check Python Path:**

```bash
# Must run from project root
pwd  # Should end with /kafka-activity-tracker

# If in wrong directory:
cd path/to/kafka-activity-tracker
```

**Check for Syntax Errors:**

```bash
# Test imports manually
python -c "from app.main import app; print('OK')"

# If errors, check specific file:
python -m app.main
```

### Dashboard Not Updating

**Symptoms:**

- Dashboard loads but shows stale data
- Polling interval not working

**Solutions:**

**Check Browser Console:**

```bash
# Open browser console (F12)
# Look for errors in JavaScript console
# Common issues:
# - Fetch requests failing (CORS)
# - Network errors
# - JavaScript syntax errors
```

**Check Endpoint:**

```bash
# Test analytics endpoint manually
curl http://localhost:8000/analytics

# Should return JSON with current metrics
```

**Verify Dashboard Code:**

```javascript
// In dashboard.html, ensure interval is running:
setInterval(updateDashboard, 1000); // Updates every second
```

**Use WebSocket Instead (Optional):**

- Implement WebSocket connection
- Push updates instead of polling
- More efficient and real-time

---

## Advanced Configuration

### Tuning Kafka Performance

**For Higher Throughput:**

```yaml
# In docker-compose.yml, add to broker environment:
KAFKA_NUM_NETWORK_THREADS: 8
KAFKA_NUM_IO_THREADS: 16
KAFKA_SOCKET_SEND_BUFFER_BYTES: 102400
KAFKA_SOCKET_RECEIVE_BUFFER_BYTES: 102400
```

**For Lower Latency:**

```python
# In app/kafka/producer.py:
linger_ms=0  # Send immediately, don't batch
compression_type='none'  # Skip compression
```

### Custom Topic Configuration

**Create Topic with Specific Settings:**

```python
# In scripts/setup_topics.py:
NewTopic(
    name="custom-topic",
    num_partitions=6,
    replication_factor=3,
    topic_configs={
        'retention.ms': '86400000',  # 1 day
        'segment.ms': '3600000',  # 1 hour
        'compression.type': 'gzip',
        'min.insync.replicas': '2',
    }
)
```

### Monitoring and Metrics

**Enable JMX Metrics:**

Already configured in docker-compose.yml on ports 9101-9103.

**Access via Kafka UI:**

- Navigate to Brokers
- Click on a broker
- View metrics: CPU, memory, disk I/O

**Add Prometheus (Optional):**

- Add Prometheus to docker-compose.yml
- Configure JMX exporter
- Create dashboards in Grafana

### Running in Production Mode

**Security Hardening:**

```bash
# 1. Enable authentication (SASL/PLAIN)
# 2. Enable encryption (SSL/TLS)
# 3. Set up ACLs (Access Control Lists)
# 4. Use proper network segmentation
```

**Deployment Checklist:**

- [ ] Change default passwords
- [ ] Enable authentication
- [ ] Configure SSL certificates
- [ ] Set up monitoring
- [ ] Configure log aggregation
- [ ] Plan backup strategy
- [ ] Document disaster recovery

---

## Quick Reference

### Common Commands

```bash
# Start Kafka cluster
docker-compose up -d

# Stop Kafka cluster
docker-compose down

# View logs
docker-compose logs -f kafka-1

# Restart specific service
docker-compose restart kafka-2

# Check status
docker-compose ps

# Start application
uvicorn app.main:app --reload

# Run tests
pytest tests/

# Create topics
python scripts/setup_topics.py

# Generate load
python tests/load/generate_load.py
```

### Useful Kafka Commands

```bash
# List topics (inside kafka container)
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --list

# Describe topic
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic page-views

# List consumer groups
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Describe consumer group
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group analytics-processor-group
```

### Key URLs

- **Application**: http://localhost:8000
- **Dashboard**: http://localhost:8000/dashboard
- **API Docs**: http://localhost:8000/docs
- **Kafka UI**: http://localhost:8080

---

## Getting Help

If you encounter issues not covered here:

1. **Check Project Documentation**

   - [Architecture.md](Architecture.md) - System design
   - [Testing-Strategy.md](Testing-Strategy.md) - Testing approach
   - [Project-Progress.md](Project-Progress.md) - Current status

2. **Check Official Docs**

   - [Kafka Documentation](https://kafka.apache.org/documentation/)
   - [FastAPI Documentation](https://fastapi.tiangolo.com/)
   - [Docker Documentation](https://docs.docker.com/)

3. **Search Online**

   - Stack Overflow: [apache-kafka tag](https://stackoverflow.com/questions/tagged/apache-kafka)
   - GitHub Issues: Search similar projects

4. **Team Communication**
   - Post in team Slack/Discord
   - Email team members
   - Schedule debugging session

---

**Last Updated:** December 2, 2024  
**Maintained By:** Kafka Activity Tracker Team

For updates or corrections to this guide, please create an issue or pull request.

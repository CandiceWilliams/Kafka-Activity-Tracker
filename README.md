# :bar_chart: Kafka Activity Tracker

> A real-time website activity tracking system built with Apache Kafka, demonstrating streaming data capabilities for database systems exploration.

[![Kafka](https://img.shields.io/badge/Kafka-7.5.0-black?logo=apache-kafka)](https://kafka.apache.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104.1-009688?logo=fastapi)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?logo=python)](https://www.python.org/)

---

## Overview

This project explores Apache Kafka's architecture and capabilities through a practical implementation of real-time website activity tracking. The system captures user interactions (clicks, page views, slider inputs, etc.) and processes them in real-time using Kafka Streams, displaying live statistics on a web dashboard.

### What This Project Demonstrates

- ✅ **Event Streaming** - Real-time capture and processing of user activities
- ✅ **Kafka Architecture** - Multi-broker cluster with topic partitioning and replication
- ✅ **Consumer Groups** - Parallel processing with multiple consumer groups
- ✅ **Fault Tolerance** - System resilience when brokers fail
- ✅ **Stream Processing** - Real-time aggregation and analytics
- ✅ **Scalability** - Performance testing across different configurations

---

## 🏗️ Architecture

### System Overview

```
┌─────────────┐
│   Browser   │ ← User interactions (clicks, inputs, etc.)
└──────┬──────┘
       │ HTTP POST /events/*
       ▼
┌──────────────────┐
│  FastAPI Server  │ ← Event validation & routing
└──────┬───────────┘
       │ Kafka Producer
       ▼
┌─────────────────────────────────────────┐
│         Kafka Cluster (3 Brokers)        │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐ │
│  │ Broker 1│  │ Broker 2│  │ Broker 3│ │ ← Fault tolerance
│  └─────────┘  └─────────┘  └─────────┘ │
│                                          │
│  Topics: page-views, button-clicks,     │
│          slider-events, dropdown-       │
│          selections, text-inputs,       │
│          toggle-events, user-sessions,  │
│          analytics-results              │
└─────────────────┬───────────────────────┘
                  │
      ┌───────────┴───────────┐
      │                       │
      ▼                       ▼
┌──────────────┐    ┌──────────────────┐
│  Analytics   │    │   Dashboard      │
│  Consumer    │    │   Consumer       │
│  Group       │    │   Group          │
└──────┬───────┘    └────────┬─────────┘
       │                     │
       ▼                     ▼
┌──────────────┐    ┌──────────────────┐
│ Aggregates   │    │   Real-time      │
│ Statistics   │    │   Dashboard      │
└──────────────┘    └──────────────────┘
```

### Key Components

- **3-Broker Kafka Cluster** (KRaft mode - no Zookeeper)
- **8 Topic Types** (organized by event type with different partition counts)
- **2 Consumer Groups** (analytics processing & dashboard updates)
- **FastAPI Backend** (event validation and API endpoints)
- **Web Dashboard** (real-time visualization)
- **Kafka UI** (cluster monitoring and management)

---

## 🚀 Quick Start

### Prerequisites

Ensure you have these installed:

| Tool           | Version | Check Command              |
| -------------- | ------- | -------------------------- |
| Docker         | 20.10+  | `docker --version`         |
| Docker Compose | 2.0+    | `docker-compose --version` |
| Python         | 3.9+    | `python --version`         |
| Git            | Any     | `git --version`            |

### Setup in 5 Steps

```bash
# 1. Clone the repository
git clone https://github.com/your-username/kafka-activity-tracker.git
cd kafka-activity-tracker

# 2. Start Kafka cluster (3 brokers)
docker-compose up -d

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Create Kafka topics
python scripts/setup_topics.py

# 5. Run the application
uvicorn app.main:app --reload
```

### Access the Application

Once running, access these URLs:

- 🌐 **Activity Tracker**: http://localhost:8000
- 📊 **Analytics Dashboard**: http://localhost:8000/dashboard
- 🔧 **Kafka UI**: http://localhost:8080
- 📖 **API Docs**: http://localhost:8000/docs

---

## 📚 Detailed Documentation

For comprehensive setup instructions, troubleshooting, and configuration details:

👉 **[Complete Setup Guide](docs/Setup-Guide.md)**

---

## Project Goals

1. **Architecture Exploration**: Understanding Kafka's data flow, scalability, and design principles
2. **Feature Investigation**: Data storage, query optimization, security, and APIs
3. **Case Study**: Real-world implementation of activity tracking
4. **Testing**: Fault tolerance, security, scalability, and data flow validation

## Technology Stack

### Backend

- **Apache Kafka 7.5.0** (KRaft mode)
- **FastAPI 0.104.1** (async web framework)
- **kafka-python 2.0.2** (Kafka client library)
- **Pydantic 2.5.0** (data validation)

### Frontend

- **HTML5** with Tailwind CSS
- **Vanilla JavaScript** (event handling)
- **Plotly.js** (data visualization)
- **WebSockets** (real-time updates)

### Infrastructure

- **Docker & Docker Compose** (containerization)
- **Kafka UI** (cluster monitoring)

### Development

- **Python 3.9+** (application code)
- **pytest** (testing framework)
- **Git** (version control)

## Team Members

- Candice Williams
- Julian
- Slava

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Documentation:](https://docs.confluent.io/)
- "_Kafka: The Definitive Guide_" by Narkhede, Shapira, and Palino
- "_Mastering Kafka Streams and ksqlDB_" by Mitch Seymour

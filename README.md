# Kafka Activity Tracker

> A real-time website activity tracking system built on Apache Kafka, exploring streaming architecture, fault tolerance, and stream processing as an alternative to traditional database-centric systems.

[![Kafka](https://img.shields.io/badge/Kafka-7.5.0-black?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![ksqlDB](https://img.shields.io/badge/ksqlDB-stream%20processing-0073BB?logo=ksql&logoColor=white)](https://ksqldb.io/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104.1-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Pydantic](https://img.shields.io/badge/Pydantic-validation-E92063?logo=pydantic&logoColor=white)](https://docs.pydantic.dev/)

---

## Overview

This project was built for a Databases II (Advanced Databases) course to explore why event-driven, stream-processing architectures like Apache Kafka exist as an alternative to traditional relational database systems. Relational DBMS excel at structured, batch-updated data, but are not well suited to continuous, high-volume event streams such as system monitoring, financial transactions, or user activity tracking.

To investigate this gap, we built a working case study: a real-time website activity tracker that captures user interactions (clicks, page views, slider inputs, and more), streams them through a 3-broker Kafka cluster, processes them with ksqlDB, and visualizes live statistics on a web dashboard.

### What This Project Demonstrates

- **Event Streaming** — real-time capture and processing of user activity events
- **Distributed Architecture** — a 3-broker Kafka cluster running in KRaft mode (no ZooKeeper)
- **Partitioning & Parallelism** — topic partitioning to enable parallel consumer throughput
- **Consumer Groups** — independent analytics and dashboard consumer groups processing the same streams
- **Stream Processing** — real-time aggregation via ksqlDB
- **Fault Tolerance** — producer/consumer recovery behavior under a full broker outage
- **Security & Validation** — Pydantic-based input validation at the API boundary

---

## Architecture

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
│ (ksqlDB)     │    │   (WebSockets)   │
└──────────────┘    └──────────────────┘
```

### Key Components

- **3-Broker Kafka Cluster** (KRaft mode)
- **8 Topic Types**, organized by event type with independent partition counts
- **2 Consumer Groups** — analytics processing and live dashboard updates
- **FastAPI Backend** — event validation and REST API
- **ksqlDB** — SQL-based real-time stream aggregation
- **Web Dashboard** — real-time visualization over WebSockets
- **Kafka UI** — cluster monitoring and management

---

## Quick Start

### Prerequisites

| Tool           | Version | Check Command              |
| -------------- | ------- | --------------------------- |
| Docker         | 20.10+  | `docker --version`          |
| Docker Compose | 2.0+    | `docker-compose --version`  |
| Python         | 3.9+    | `python --version`          |
| Git             | Any     | `git --version`             |

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/your-username/kafka-activity-tracker.git
cd kafka-activity-tracker

# 2. Start the Kafka cluster (3 brokers)
docker-compose up -d

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Create Kafka topics
python scripts/setup_topics.py

# 5. Run the application
uvicorn app.main:app --reload
```

### Access the Application

| Service | URL |
|---|---|
| Activity Tracker | http://localhost:8000 |
| Analytics Dashboard | http://localhost:8000/dashboard |
| Kafka UI | http://localhost:8080 |
| API Docs | http://localhost:8000/docs |

For comprehensive setup instructions, troubleshooting, and configuration details, see the [Complete Setup Guide](docs/Setup-Guide.md).

---

## System Testing

Four tests were designed to validate Kafka's core claims around scalability, security, and fault tolerance. Full methodology and analysis are in the [final report](docs/CS4411___Kafka_Final_Report.pdf); summarized results below.

### Scalability
Throughput was measured across 1–6 partitions (5,000 events per run, replication factor 1).

| Partitions/Consumers | Duration (s) | Throughput (events/s) |
|---|---|---|
| 1 | 67.27 | 74.32 |
| 2 | 63.77 | 78.41 |
| 3 | 59.82 | 83.59 |
| 4 | 58.81 | 85.01 |
| 5 | 56.46 | 88.25 |
| 6 | 56.43 | 88.60 |

Throughput improved with additional partitions but tapered off after 3, indicating that producer-side latency (HTTP handling and FastAPI processing), not Kafka itself, was the bottleneck — a reminder that partitioning alone doesn't guarantee linear scaling.

### Input Validation
Three categories of malformed events (invalid event type, out-of-range value, wrong data type) were sent to the `/event` endpoint. All three were correctly rejected by Pydantic validation with the expected HTTP error codes, and no invalid records reached Kafka.

### Consumer Error Recovery
A consumer was sent a valid event, then an intentionally malformed event (`details = None`), then another valid event. The consumer logged the error, did not crash, and resumed normal processing on the next valid event — confirming that a single bad record can't take down a consumer group.

### Broker Outage Recovery
All 3 brokers were paused via Docker while events continued to be sent. Kafka's internal logs showed expected failure signals (expired heartbeats, coordinator loss, failed offset commits) even though `producer.send()` continued returning `200 OK` (Kafka producers queue asynchronously). On broker recovery, leader election, consumer rebalancing, and coordinator reassignment all completed automatically with no manual intervention required.

---

## Project Goals

1. **Architecture Exploration** — understand Kafka's data flow, scalability, and design principles
2. **Feature Investigation** — data storage, query optimization, security, and APIs
3. **Case Study** — a real-world implementation of activity tracking
4. **Testing** — validate fault tolerance, security, scalability, and data flow

## Technology Stack

**Backend**
- Apache Kafka 7.5.0 (KRaft mode)
- ksqlDB (real-time SQL stream processing)
- FastAPI 0.104.1 (async web framework)
- kafka-python 2.0.2 (Kafka client library)
- Pydantic 2.5.0 (data validation)

**Frontend**
- HTML5 with Tailwind CSS
- Vanilla JavaScript (event handling)
- Plotly.js (data visualization)
- WebSockets (real-time updates)

**Infrastructure**
- Docker & Docker Compose (containerization)
- Kafka UI (cluster monitoring)

**Development**
- Python 3.9+
- pytest (testing framework)
- Git (version control)

## Team Members

- Candice Williams
- Julian Sharpe
- Vladislav Zagidulin

## Report

Full write-up, including architecture rationale, implementation challenges, and complete test methodology, is available in [`docs/CS4411___Kafka_Final_Report.pdf`](docs/CS4411___Kafka_Final_Report.pdf).

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Documentation](https://docs.confluent.io/)
- [ksqlDB Documentation](https://docs.ksqldb.io/)
- Narkhede, N., Shapira, G., & Palino, T. *Kafka: The Definitive Guide.* O'Reilly Media, 2017.
- Seymour, M. *Mastering Kafka Streams and ksqlDB: Building Real-Time Data Systems by Example.* O'Reilly Media, 2021.

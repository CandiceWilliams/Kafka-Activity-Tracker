# Project Architecture Breakdown

This is a detailed breakdown of our Kafka workflow that we are using in this project

## Components

1. Topics:
   - `page-views`
   - `clicks`
   - `searches`
   - `user-sessions`
   - `analytics-output`
2. Brokers: 1 broker (single node cluster for development, may be expanded later)
   - //TODO: add 3+ for fault tolerance
3. Partitions:
   - page-views: 3 partitions (high volume expected)
   - clicks: 3 partitions (high volume)
   - searches: 2 partitions (moderate volume)
   - user-sessions: 2 partitions (lower volume, session-based)
   - analytics-output: 1 partition (aggregated results)
4. Consumer Groups:
   - `analytics-processor-group`: Processes events for real-time analytics
   - `dashboard-consumer-group`: Feeds data to the dashboard
   - `audit-consumer-group`: Optional, for logging/auditing

## Repository Structure

```ascii

KafkaActivityTracker/
├── app/
│   ├── __init__.py
│   ├── main.py                    # FastAPI entry point
│   ├── config.py                  # Configuration management
│   ├── models/
│   │   ├── __init__.py
│   │   └── events.py              # Pydantic event models
│   ├── kafka/
│   │   ├── __init__.py
│   │   ├── producer.py            # Topic-based producer
│   │   ├── consumer.py            # Base consumer class
│   │   └── admin.py               # Topic creation
│   ├── services/
│   │   ├── __init__.py
│   │   ├── analytics_service.py   # Analytics logic
│   │   └── state_store.py         # Persistent state
│   ├── api/
│   │   ├── __init__.py
│   │   ├── routes.py              # API endpoints
│   │   └── websocket.py           # WebSocket handler
│   └── templates/
│       ├── index.html             # Activity tracker page
│       └── dashboard.html         # Analytics dashboard
├── tests/
│   ├── integration/
│   │   ├── test_fault_tolerance.py
│   │   ├── test_scalability.py
│   │   └── test_latency.py
│   └── load/
│       └── generate_load.py
├── scripts/
│   ├── setup_topics.py            # Initialize Kafka topics
│   └── generate_load.py           # Test data generation
├── docs/
│   ├── Architecture.md
│   ├── Setup-Guide.md
│   ├── Testing-Strategy.md
│   ├── Kafka-Concepts.md
│   └── Project-Progress.md
├── docker/
│   ├── docker-compose.yml         # 3-broker cluster
│   └── docker-compose.dev.yml     # Dev overrides
├── .env.example
├── .gitignore
├── requirements.txt
├── requirements-dev.txt
├── README.md
└── Makefile (optional)
```

**Key Organizational Principles:**

1. **Separation of Concerns:**

   - `app/kafka/` - All Kafka interaction code
   - `app/services/` - Business logic
   - `app/api/` - HTTP/WebSocket endpoints
   - `app/models/` - Data validation

2. **Configuration Management:**

   - `config.py` - Single source of truth
   - `.env` files - Environment-specific settings
   - No hardcoded values

3. **Testing Structure:**

   - `tests/integration/` - End-to-end tests
   - `tests/load/` - Performance tests
   - Separate from application code

4. **Documentation:**
   - `docs/` - All project documentation
   - Separate from code for clarity
   - Easy to reference

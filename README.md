# 🌆 UrbanPulse - Real-Time Smart City Data Streaming

UrbanPulse is an end-to-end event-driven data streaming pipeline designed to monitor urban metrics—including traffic signal flows, air quality (AQI), and energy consumption—across city zones in real time.

---

## 🛠️ Tech Stack & Architecture

* **Message Broker:** Apache Kafka (Docker Compose)
* **Stream Processing Engine:** Apache Spark Structured Streaming / Apache Flink
* **Data Storage / Sink:** Apache Parquet (Partitioned by Zone/Ward)
* **Language / Environment:** Python 3.12 / PySpark / Confluent Kafka

---

## 📋 Prerequisites

Ensure you have the following installed before running the project:
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with Docker Compose)
* Python 3.10 or higher

---

## 🚀 Step-by-Step Execution Guide

### Step 1: Start the Kafka Infrastructure
Open your terminal in the project root directory and spin up the Kafka and ZooKeeper containers:

```bash
docker-compose up -d
```

> **Verification:** Run docker ps to ensure the Kafka container is running on port 9092

### Step 2: Set Up Python Virtual Environment

```
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Step 3: Run Data Producer to stream Multi-Sensor Telemetry (Bus GPS, Air Quality, Smart Meters):
```
python code/producers/urban_producers.py
```
### To stream Traffic Signals:
```
python code/producers/traffic_producer.py
```
### To run Dead Letter Queue (DLQ) validation:
```
python code/producers/dlq_validator.py
```
### Step 4: Run Stream Consumer / Processing Pipeline
```
python code/spark/spark_ward_analytics.py
```
### Spark - AQI Advisories Stream:
```
python code/spark/spark_aqi_advisories.py
```
### Kafka Consumers / Enrichment:
```
python code/kafka/priority_consumers.py
```
### Flink - Incident Detector:
```
python code/flink/flink_incident_detector.py
```

### Step 5: Verify Output & Sinks
```
python code/analysis/parquet_reader.py
```

## repo structure
` ```text `
urbanpulse-kafka/
├── code/
│   ├── analysis/              
│   │   └── parquet_reader.py         # Output verification script
│   ├── flink/                 
│   │   └── flink_incident_detector.py # Flink real-time incident detection
│   ├── kafka/                 
│   │   ├── create_topics.sh          # Topic setup script
│   │   ├── priority_consumers.py     # Kafka consumer logic
│   │   └── route_enrichment_stream.py# Stream enrichment handler
│   ├── producers/             
│   │   ├── dlq_validator.py          # DLQ validation utility
│   │   ├── traffic_producer.py       # Traffic signals producer
│   │   └── urban_producers.py        # Multi-sensor telemetry producer
│   └── spark/                 
│       ├── spark_aqi_advisories.py   # Spark AQI alert processing
│       └── spark_ward_analytics.py   # Spark ward aggregations
├── data/                      # Output sinks (Parquet files)
├── docker-compose.yml         # Kafka & ZooKeeper cluster config
├── requirements.txt           # Python dependencies
└── README.md                  # Project evaluation guide

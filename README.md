\# 🌆 UrbanPulse - Real-Time Smart City Data Streaming



UrbanPulse is an end-to-end event-driven data pipeline designed to monitor urban environmental metrics (AQI, PM2.5, PM10) and energy usage across city zones in real time.



\---



\## 🛠️ Tech Stack \& Architecture

\* \*\*Message Broker:\*\* Apache Kafka (Docker Compose)

\* \*\*Stream Processing Engine:\*\* Apache Spark Structured Streaming (PySpark)

\* \*\*Data Storage / Sink:\*\* Apache Parquet (Partitioned by Zone/Ward)

\* \*\*Language / Environment:\*\* Python 3.12 / PySpark 3.5



\---



\## 🚀 How to Run the Pipeline



\### 1. Start Kafka Cluster

```bash

docker-compose up -d


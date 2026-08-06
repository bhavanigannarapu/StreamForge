📖 Overview

StreamForge is a Python-native distributed stream processing system designed to process high-volume real-time event streams efficiently and reliably.

The system ingests streaming data through Apache Kafka, distributes processing across multiple worker nodes, maintains application state using RocksDB, and automatically recovers from failures through partition rebalancing and state restoration.

The project demonstrates enterprise-grade distributed systems concepts including:

- Exactly-once processing
- Stateful stream processing
- Fault tolerance
- Automatic recovery
- Windowed aggregations
- High-throughput event streaming


🎯 Problem Statement

Processing millions of real-time events using Python while maintaining fault tolerance, state management, and exactly-once guarantees is challenging.

StreamForge addresses this challenge by building a distributed event processing engine capable of recovering from worker failures without losing or duplicating events.



🌍 Use Case

An IoT fleet management company collects temperature readings from thousands of trucks every few seconds.

StreamForge:

- Receives sensor data through Apache Kafka
- Distributes processing across multiple worker nodes
- Calculates rolling temperature averages
- Stores application state in RocksDB
- Automatically rebalances partitions if a worker crashes
- Restores state from changelog topics
- Ensures no event is lost or processed twice



✨ Features

- Fault-Tolerant Distributed Processing
- Exactly-Once Event Processing
- Automatic Partition Rebalancing
- Stateful Recovery using RocksDB
- Windowed Stream Aggregations
- High Throughput Event Processing
- Worker Health Monitoring
- Processing Metrics Dashboard
- Real-Time Stream Visualization



🛠 Tech Stack

Python - Core Development 
Apache Kafka - Message Broker 
Faust / Bytewax - Stream Processing
RocksDB - Stateful Storage 
FastAPI - Backend APIs 
React Flow - Stream Topology Dashboard 
Prometheus - Metrics Collection 


🏗 Architecture

```
                IoT Devices
                     │
                     ▼
             Apache Kafka Topics
                     │
                     ▼
          StreamForge Worker Nodes
            (Faust / Bytewax)
                     │
        ┌────────────┴────────────┐
        ▼                         ▼
  Windowed Aggregation       RocksDB State
        │                         │
        └────────────┬────────────┘
                     ▼
              FastAPI Backend
                     │
                     ▼
          React Flow Dashboard

```


🔄 Workflow

1. IoT devices continuously publish events.
2. Kafka stores and partitions incoming messages.
3. StreamForge workers consume Kafka partitions.
4. Events are filtered and processed.
5. Windowed aggregations are calculated.
6. Processing state is stored inside RocksDB.
7. If a worker crashes:
   - Kafka reassigns partitions.
   - State is restored from RocksDB changelog.
   - Processing resumes automatically.
8. Metrics are exposed to FastAPI and visualized in React Flow.


 📊 Expected Outcomes

- Zero Data Loss
- Exactly-Once Processing
- Automatic Worker Recovery
- High Availability
- Fault-Tolerant Streaming
- Scalable Distributed Processing



📂 Project Structure
```
StreamForge
│
├── produce/
├── consumer/
├── workers/
├── state_store/
├── api/
├── dashboard/
├── monitoring/
├── docker/
├── docs/
└── README.md

 Future Enhancements

- Kubernetes Deployment
- Docker Compose Support
- CI/CD Pipeline
- Authentication & Authorization
- Multi-Cluster Kafka Support
- Alerting using Grafana



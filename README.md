# FraudShield
Overview

Real-Time Fraud Detection System is an event-driven stream processing application built using Spring Boot and Kafka Streams to detect suspicious financial transactions in real time.

The system ingests transaction events, applies fraud detection rules on streaming data, and routes suspicious transactions into a fraud alert pipeline.

This project focuses on stateful stream processing, rule-based fraud detection, and distributed event-driven architecture.

Core Features (Current MVP)
Implemented / Current Scope
Transaction Event Ingestion
REST API simulates live transactions.
Transactions are published into Kafka.

Topic:

transactions
Fraud Detection Rules
Rule 1 — Large Transaction Detection

Flags transactions exceeding threshold:

amount > limit
Rule 2 — Rapid Transaction Detection

Detects suspicious velocity:

N transactions in T seconds

Implemented using:

Kafka windowing
Sliding window logic
Rule 3 — Location Anomaly Detection

Flags impossible travel patterns.

Example:

Delhi → Dubai in 2 minutes

Uses stateful stream checks.

Fraud Alert Stream

Suspicious transactions routed to:

fraud-alerts
Real-Time Alert Consumer

Consumes fraud alerts and logs/handles suspicious events

Architecture:
Transaction Producer
↓
Kafka transactions topic
↓
Kafka Streams Fraud Engine
├── Large Transaction Rule
├── Rapid Transaction Rule
└── Location Rule
↓
Kafka fraud-alerts topic
↓
Fraud Alert Consumer

Tech Stack
Java
Spring Boot
Apache Kafka
Kafka Streams
Maven
Lombok

# folder structure

fraud-detection-system/
│
├── src/main/java/com/project/fraud
│
├── config/
│   └── KafkaConfig.java
│
├── controller/
│   └── TransactionController.java
│
├── model/
│   └── Transaction.java
│
├── producer/
│   └── TransactionProducer.java
│
├── streams/
│   └── FraudDetectionStream.java
│
├── rules/
│   ├── LargeTransactionRule.java
│   ├── VelocityRule.java
│   └── LocationRule.java
│
├── consumer/
│   └── FraudAlertConsumer.java
│
└── resources/
└── application.yml

Current Implementation Details

1. Transaction Producer

Generates transaction events:

{
transactionId,
userId,
amount,
timestamp,
location
}

Publishes to Kafka.

2. Kafka Topic Configuration

Configured topics:

transactions
fraud-alerts

Includes:

Partitions
Replication
Producer settings
Stream properties
3. Stream Processing Engine

Consumes:

transactions

Processes:

Read
Filter
Detect
Route

Uses:

KStream
Filtering
Window aggregations
Stateful processing

4. Fraud Rule Engine

Rules isolated into modular classes:

Rule Interface
Rule Implementations

Allows adding rules easily.

5. Fraud Alerts

Suspicious events transformed into alert objects and sent to:

fraud-alerts
How to Run
Start Kafka

Run Zookeeper and Kafka broker.

Create topics:

transactions
fraud-alerts

Run Spring Boot:
mvn spring-boot:run

Trigger Transactions

Use API:

POST /api/transactions

Transactions flow through fraud engine.

Example Flow
POST transaction
↓
Produced to Kafka
↓
Fraud rules evaluate
↓
Suspicious event detected
↓
Alert published
↓
Alert consumer receives fraud event

Concepts Used:
Distributed Systems
Event-driven architecture
Kafka pub/sub
Partitioned processing
Consumer groups

Stream Processing:
Stateful streams
Sliding windows
Event routing

DSA Concepts
Sliding window algorithms
Hash-based state tracking
Rule evaluation logic

Future Improvements (Phase 2+)
Redis Behavioral State

Store:

last location
recent activity
user behavior history

For contextual fraud analysis.

Risk Scoring Engine

Move from binary fraud detection:

fraud / not fraud

To weighted scoring:

risk score based detection
Additional Fraud Rules

Add:

Merchant anomaly detection
Device/IP anomaly
Repeated failed transaction detection
Card testing detection
Dashboard & Monitoring

Add:

Prometheus
Grafana
Fraud metrics visualization
Microservice Decomposition

Split into:

producer-service
fraud-engine-service
alert-service
ML-Based Fraud Detection (Future)

Integrate anomaly detection models:

Isolation Forest
Behavioral models
Fraud scoring models

Future Roadmap

Phase 1 (Current MVP)

✔ Rule-based fraud detection
✔ Kafka Streams processing
✔ Real-time fraud alerts

Phase 2
Redis state
Risk scoring
More fraud rules

Phase 3
Dashboards

Microservices
ML fraud detection

Objectives

This project aims to:

Detect suspicious transactions in real time
Explore distributed stream processing
Apply rule-based fraud analytics
Build a scalable fraud detection foundation
Potential Extensions

Can evolve into:

FinTech fraud platform
Streaming analytics engine
Real-time anomaly detection system
Status
Current Version:
MVP (Phase 1) In Progress
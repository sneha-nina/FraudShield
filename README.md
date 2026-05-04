# FraudShield

Project Summary — Real-Time Fraud Detection System

Real-Time Fraud Detection System is an event-driven stream processing application built using Spring Boot and Kafka Streams to monitor and analyze financial transactions in real time. The system ingests continuous transaction events, applies fraud detection rules on streaming data, and generates instant fraud alerts for suspicious activities.

The project follows a Read → Process → Write architecture:

Read: Consume live transaction events from a Kafka topic.
Process: Analyze transactions using rule-based fraud detection logic.
Write: Publish suspicious transactions to a fraud alerts topic for downstream handling.

The initial implementation focuses on three core fraud detection scenarios:

Large Transaction Detection
Flags transactions exceeding a defined threshold amount.
Rapid Transaction Detection
Detects unusually frequent transactions within a short time window using stream windowing.
Location Anomaly Detection
Identifies suspicious location changes, such as impossible travel between geographically distant transactions.

Kafka Streams provides stateful stream processing, windowed aggregations, and partitioned parallel processing, enabling scalable and low-latency fraud analysis. Suspicious transactions are routed to a dedicated fraud-alerts topic, where alerting or response services can consume them.

Key Objectives
Process live transaction streams in real time
Detect fraud using rule-based streaming analytics
Apply stateful and window-based fraud checks
Trigger instant fraud alerts through Kafka topics
Explore event-driven architecture and distributed stream processing

Technology Stack:

Java

Spring Boot

Apache Kafka

Kafka Streams

Maven

Expected Outcome

A lightweight but scalable fraud detection engine capable of identifying suspicious transactions in real time and serving as a foundation for future enhancements such as Redis-backed state, risk scoring, and anomaly detection models.
Overview


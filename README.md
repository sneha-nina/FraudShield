# FraudShield

A real-time fraud detection system built with Spring Boot and Apache Kafka.
Detects suspicious financial transactions as they happen using event-driven
stream processing and rule-based fraud analysis.

## What It Does

FraudShield ingests live transaction events, applies fraud detection rules
on a continuous data stream, and generates instant alerts for suspicious activity.

It focuses on three core fraud patterns that are difficult to catch with
batch processing — making real-time stream processing essential.

## System Architecture
```
POST /api/transactions
↓
TransactionController   — receives transaction via REST API
↓
TransactionProducer     — publishes to Kafka transactions topic
↓
FraudDetectionStream    — evaluates all fraud rules
├── Rule 1: Large Transaction
├── Rule 2: Rapid Transactions
└── Rule 3: Location Anomaly
↓
fraud-alerts topic
↓
FraudAlertConsumer      — receives and logs fraud alerts
```

## Fraud Detection Rules

**Rule 1 — Large Transaction**
Flags any transaction exceeding a configured amount threshold (default: 10,000).
Stateless — evaluated per transaction independently.

**Rule 2 — Rapid Transactions**
Detects suspicious transaction velocity using a sliding window algorithm.
Flags a user who makes 5 or more transactions within 60 seconds.
Uses a cooldown mechanism to suppress duplicate alerts within the same burst.

**Rule 3 — Location Anomaly**
Detects impossible travel between consecutive transactions.
Calculates travel speed between two locations using the Haversine formula.
If the required speed exceeds 900 km/h (commercial flight speed), the
transaction is flagged as physically impossible.

## Tech Stack

- Java 21
- Spring Boot 3.4.1
- Apache Kafka 4.2.0 (KRaft mode — no Zookeeper)
- Kafka Streams
- Maven

## Running Locally

**1. Start Kafka**
```bash
# Set heap options to bypass wmic issue on Windows 11
set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G

# Start Kafka in KRaft mode
D:\Kafka\bin\windows\kafka-server-start.bat D:\Kafka\config\server.properties
```

**2. Create Topics**
```bash
kafka-topics.bat --create --topic transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
kafka-topics.bat --create --topic fraud-alerts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

**3. Run the Application**
```bash
mvn spring-boot:run
```

**4. Simulate Transactions**
```bash
# Normal transaction
curl -X POST http://localhost:8080/api/transactions \
  -H "Content-Type: application/json" \
  -d '{"userId":"user1","amount":500.00,"location":"Delhi"}'

# Large transaction fraud
curl -X POST http://localhost:8080/api/transactions \
  -H "Content-Type: application/json" \
  -d '{"userId":"user1","amount":15000.00,"location":"Delhi"}'

# Location anomaly fraud (run immediately after above)
curl -X POST http://localhost:8080/api/transactions \
  -H "Content-Type: application/json" \
  -d '{"userId":"user1","amount":200.00,"location":"Dubai"}'
```

## Configuration

All fraud rule thresholds are configurable in `application.yml`:

```yaml
fraud:
  rules:
    large-transaction-threshold: 10000.00
    velocity-transaction-count: 5
    velocity-window-seconds: 60
```

## Project Structure
```
src/main/java/com/example/fraud_detection_system/
├── config/          — Kafka topic configuration
├── controller/      — REST API for transaction ingestion
├── model/           — Transaction and FraudAlert data models
├── producer/        — Publishes transactions to Kafka
├── streams/         — Kafka Streams fraud detection pipeline
├── rules/           — Isolated fraud rule implementations
└── consumer/        — Consumes and handles fraud alerts
```

## Future Improvements

- Risk scoring engine — weighted score instead of binary fraud/not-fraud
- Redis-backed behavioral state — track user history across sessions
- Additional rules — merchant anomaly, device fingerprinting, card testing detection
- Prometheus + Grafana dashboard for fraud metrics
- ML-based anomaly detection
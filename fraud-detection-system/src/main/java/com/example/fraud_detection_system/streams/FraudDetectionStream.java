package com.example.fraud_detection_system.streams;

import com.example.fraud_detection_system.model.FraudAlert;
import com.example.fraud_detection_system.model.RiskScore;
import com.example.fraud_detection_system.model.Transaction;
import com.example.fraud_detection_system.scoring.RiskScoreEngine;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class FraudDetectionStream {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionStream.class);
    private static final String INPUT_TOPIC = "transactions";
    private static final String OUTPUT_TOPIC = "fraud-alerts";
    private static final String LOCATION_STORE = "location-state-store";
    private static final String VELOCITY_STORE = "velocity-count-store";

    private final RiskScoreEngine riskScoreEngine;

    public FraudDetectionStream(RiskScoreEngine riskScoreEngine) {
        this.riskScoreEngine = riskScoreEngine;
    }

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        JsonSerde<Transaction> transactionSerde = new JsonSerde<>(Transaction.class);
        JsonSerde<FraudAlert> fraudAlertSerde = new JsonSerde<>(FraudAlert.class);

        // State store — last known location + timestamp per user
        StoreBuilder<KeyValueStore<String, String>> locationStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(LOCATION_STORE),
                        Serdes.String(),
                        Serdes.String());
        streamsBuilder.addStateStore(locationStoreBuilder);

        // State store — velocity count per user
        StoreBuilder<KeyValueStore<String, Long>> velocityStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(VELOCITY_STORE),
                        Serdes.String(),
                        Serdes.Long());
        streamsBuilder.addStateStore(velocityStoreBuilder);

        // Single processing pipeline
        streamsBuilder
                .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), transactionSerde))
                .process(() -> new Processor<String, Transaction, String, FraudAlert>() {

                    private KeyValueStore<String, String> locationStore;
                    private KeyValueStore<String, Long> velocityStore;
                    private ProcessorContext<String, FraudAlert> context;

                    @Override
                    public void init(ProcessorContext<String, FraudAlert> context) {
                        this.context = context;
                        this.locationStore = context.getStateStore(LOCATION_STORE);
                        this.velocityStore = context.getStateStore(VELOCITY_STORE);
                    }

                    @Override
                    public void process(Record<String, Transaction> record) {
                        Transaction transaction = record.value();
                        String userId = transaction.userId();
                        long currentTimestamp = transaction.timestamp().toEpochMilli();

                        // Read last location
                        String storedLocation = locationStore.get(userId);
                        String lastLocation = null;
                        long lastTimestamp = 0;

                        if (storedLocation != null) {
                            String[] parts = storedLocation.split("\\|");
                            lastLocation = parts[0];
                            lastTimestamp = Long.parseLong(parts[1]);
                        }

                        // Read and increment velocity count with time-based reset
                        Long currentCount = velocityStore.get(userId);
                        long newCount;

                        if (currentCount == null) {
                            // First transaction for this user
                            newCount = 1L;
                        } else {
                            // Check if window has expired using last timestamp
                            long timeDifferenceMs = currentTimestamp - lastTimestamp;
                            long windowMs = 60 * 1000L;

                            if (timeDifferenceMs > windowMs) {
                                // Window expired — reset counter
                                newCount = 1L;
                            } else {
                                // Within window — increment
                                newCount = currentCount + 1;
                            }
                        }

                        velocityStore.put(userId, newCount);

                        // Evaluate all rules via RiskScoreEngine
                        RiskScore riskScore = riskScoreEngine.evaluate(
                                transaction, newCount, lastLocation, lastTimestamp);

                        // Only produce alert if score > 0
                        if (riskScore.totalScore() > 0) {
                            log.warn("FRAUD DETECTED — userId: {} | score: {} | level: {} | rules: {}",
                                    userId,
                                    riskScore.totalScore(),
                                    riskScore.riskLevel(),
                                    riskScore.triggeredRules());

                            FraudAlert alert = new FraudAlert(
                                    UUID.randomUUID().toString(),
                                    userId,
                                    FraudAlert.AlertType.valueOf(riskScore.triggeredRules().get(0)),
                                    "Risk Score: " + riskScore.totalScore()
                                            + " | Level: " + riskScore.riskLevel()
                                            + " | Rules: " + riskScore.triggeredRules(),
                                    Instant.now());

                            context.forward(new Record<>(userId, alert, currentTimestamp));
                        }

                        // Always update location store
                        locationStore.put(userId,
                                transaction.location() + "|" + currentTimestamp);
                    }
                }, LOCATION_STORE, VELOCITY_STORE)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), fraudAlertSerde));
    }
}
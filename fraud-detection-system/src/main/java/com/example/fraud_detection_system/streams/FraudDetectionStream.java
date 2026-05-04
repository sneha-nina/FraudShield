package com.example.fraud_detection_system.streams;

import com.example.fraud_detection_system.model.FraudAlert;
import com.example.fraud_detection_system.model.Transaction;
import com.example.fraud_detection_system.rules.LargeTransactionRule;
import com.example.fraud_detection_system.rules.LocationRule;
import com.example.fraud_detection_system.rules.VelocityRule;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static java.lang.ProcessBuilder.Redirect.to;

@Component
public class FraudDetectionStream {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionStream.class);
    private static final String INPUT_TOPIC = "transactions";
    private static final String OUTPUT_TOPIC = "fraud-alerts";
    private static final String LOCATION_STORE = "location-state-store";

    private final LargeTransactionRule largeTransactionRule;
    private final VelocityRule velocityRule;
    private final LocationRule locationRule;

    @Value("${fraud.rules.velocity-window-seconds}")
    private int velocityWindowSeconds;

    public FraudDetectionStream(LargeTransactionRule largeTransactionRule,
                                VelocityRule velocityRule,
                                LocationRule locationRule) {
        this.largeTransactionRule = largeTransactionRule;
        this.velocityRule = velocityRule;
        this.locationRule = locationRule;
    }

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        JsonSerde<Transaction> transactionSerde = new JsonSerde<>(Transaction.class);
        JsonSerde<FraudAlert> fraudAlertSerde = new JsonSerde<>(FraudAlert.class);

        // Register state store for location tracking
        StoreBuilder<KeyValueStore<String, String>> locationStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(LOCATION_STORE),
                        Serdes.String(),
                        Serdes.String());
        streamsBuilder.addStateStore(locationStoreBuilder);
        //duplication alert fix
        StoreBuilder<KeyValueStore<String, Long>> deduplicationStoreBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("velocity-dedup-store"),
                        Serdes.String(),
                        Serdes.Long());
        streamsBuilder.addStateStore(deduplicationStoreBuilder);

        KStream<String, Transaction> transactionStream = streamsBuilder
                .stream(INPUT_TOPIC, Consumed.with(Serdes.String(), transactionSerde));

        // Rule 1 — Large Transaction
        transactionStream
                .filter((key, transaction) -> largeTransactionRule.isFraudulent(transaction))
                .peek((key, transaction) -> log.warn("FRAUD DETECTED [Large Transaction] userId: {} amount: {}",
                        transaction.userId(), transaction.amount()))
                .mapValues(transaction -> new FraudAlert(
                        UUID.randomUUID().toString(),
                        transaction.userId(),
                        FraudAlert.AlertType.LARGE_TRANSACTION,
                        "Transaction amount " + transaction.amount() + " exceeds threshold",
                        Instant.now()))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), fraudAlertSerde));

        // Rule 2 — Rapid Transactions (Sliding Window)
        transactionStream
                .groupByKey()
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(
                        Duration.ofSeconds(velocityWindowSeconds),
                        Duration.ofSeconds(5)))
                .count(Materialized.as("velocity-count-store"))
                .toStream()
                .filter((windowedKey, count) -> velocityRule.isFraudulent(count))
                .process(() -> new VelocityAlertProcessor(velocityWindowSeconds), "velocity-dedup-store")
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), fraudAlertSerde));
        // Rule 3 — Location Anomaly Detection
        transactionStream
                .process(() -> new org.apache.kafka.streams.processor.api.Processor<String, Transaction, String, FraudAlert>() {

                    private KeyValueStore<String, String> locationStore;
                    private org.apache.kafka.streams.processor.api.ProcessorContext<String, FraudAlert> context;

                    @Override
                    public void init(org.apache.kafka.streams.processor.api.ProcessorContext<String, FraudAlert> context) {
                        this.context = context;
                        this.locationStore = context.getStateStore(LOCATION_STORE);
                    }

                    @Override
                    public void process(org.apache.kafka.streams.processor.api.Record<String, Transaction> record) {
                        Transaction transaction = record.value();
                        String userId = transaction.userId();
                        String currentLocation = transaction.location();
                        long currentTimestamp = transaction.timestamp().toEpochMilli();

                        String storedValue = locationStore.get(userId);

                        if (storedValue != null) {
                            // Format stored: "location|timestamp"
                            String[] parts = storedValue.split("\\|");
                            String lastLocation = parts[0];
                            long lastTimestamp = Long.parseLong(parts[1]);

                            if (locationRule.isImpossibleTravel(
                                    lastLocation, currentLocation,
                                    lastTimestamp, currentTimestamp)) {

                                log.warn("FRAUD DETECTED [Location Anomaly] userId: {} | {} -> {} ",
                                        userId, lastLocation, currentLocation);

                                FraudAlert alert = new FraudAlert(
                                        UUID.randomUUID().toString(),
                                        userId,
                                        FraudAlert.AlertType.LOCATION_ANOMALY,
                                        "Impossible travel detected: " + lastLocation + " -> " + currentLocation,
                                        Instant.now());

                                context.forward(new org.apache.kafka.streams.processor.api.Record<>(
                                        userId, alert, currentTimestamp));
                            }
                        }

                        // Always update state store with latest location and timestamp
                        locationStore.put(userId, currentLocation + "|" + currentTimestamp);
                    }
                }, LOCATION_STORE)
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), fraudAlertSerde));
    }
}
package com.example.fraud_detection_system.streams;

import com.example.fraud_detection_system.model.FraudAlert;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.UUID;

public class VelocityAlertProcessor implements Processor<Windowed<String>, Long, String, FraudAlert> {

    private static final Logger log = LoggerFactory.getLogger(VelocityAlertProcessor.class);

    private final int velocityWindowSeconds;
    private KeyValueStore<String, Long> dedupStore;
    private ProcessorContext<String, FraudAlert> context;

    public VelocityAlertProcessor(int velocityWindowSeconds) {
        this.velocityWindowSeconds = velocityWindowSeconds;
    }

    @Override
    public void init(ProcessorContext<String, FraudAlert> context) {
        this.context = context;
        this.dedupStore = context.getStateStore("velocity-dedup-store");
    }

    @Override
    public void process(Record<Windowed<String>, Long> record) {
        String userId = record.key().key();
        long currentTimestamp = record.timestamp();
        long cooldownMs = velocityWindowSeconds * 1000L;

        Long lastAlertTime = dedupStore.get(userId);

        // Only fire if no alert was sent within the cooldown period
        if (lastAlertTime == null || (currentTimestamp - lastAlertTime) > cooldownMs) {
            dedupStore.put(userId, currentTimestamp);

            log.warn("FRAUD DETECTED [Rapid Transactions] userId: {} count: {}",
                    userId, record.value());

            FraudAlert alert = new FraudAlert(
                    UUID.randomUUID().toString(),
                    userId,
                    FraudAlert.AlertType.RAPID_TRANSACTIONS,
                    "User made " + record.value() + " transactions within "
                            + velocityWindowSeconds + " seconds",
                    Instant.now());

            context.forward(new Record<>(userId, alert, currentTimestamp));
        }
    }
}
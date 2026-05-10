package com.example.fraud_detection_system.simulator;

import com.example.fraud_detection_system.model.Transaction;
import com.example.fraud_detection_system.producer.TransactionProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Component
public class TransactionSimulator {

    private static final Logger log = LoggerFactory.getLogger(TransactionSimulator.class);

    private final TransactionProducer transactionProducer;
    private final Random random = new Random();

    // Test user pool
    private static final List<String> USER_IDS = List.of(
            "sim-user1", "sim-user2", "sim-user3",
            "sim-user4", "sim-user5");

    // Location pool
    private static final List<String> LOCATIONS = List.of(
            "Delhi", "Mumbai", "Dubai", "London", "NewYork", "Singapore");

    // Rapid fire users — these users simulate velocity fraud
    private static final List<String> RAPID_USERS = List.of(
            "rapid-user1", "rapid-user2");

    private int rapidFireCount = 0;

    public TransactionSimulator(TransactionProducer transactionProducer) {
        this.transactionProducer = transactionProducer;
    }

    // Fires every 2-5 seconds (simulated via fixed delay + random sleep)
    @Scheduled(fixedDelay = 2000)
    public void simulate() throws InterruptedException {
        // Random delay between 0-3 seconds for irregular intervals
        Thread.sleep(random.nextInt(3000));

        int scenario = random.nextInt(100);

        if (scenario < 70) {
            // 70% — normal transaction
            simulateNormalTransaction();
        } else if (scenario < 85) {
            // 15% — large transaction fraud
            simulateLargeTransaction();
        } else if (scenario < 95) {
            // 10% — location anomaly fraud
            simulateLocationAnomaly();
        } else {
            // 5% — rapid transaction fraud
            simulateRapidTransactions();
        }
    }

    private void simulateNormalTransaction() {
        String userId = randomUser();
        Transaction transaction = new Transaction(
                UUID.randomUUID().toString(),
                userId,
                100 + random.nextDouble() * 900, // amount between 100-1000
                Instant.now(),
                randomLocation());

        log.info("SIM [Normal] userId: {} | amount: {}", userId, transaction.amount());
        transactionProducer.sendTransaction(transaction);
    }

    private void simulateLargeTransaction() {
        String userId = randomUser();
        Transaction transaction = new Transaction(
                UUID.randomUUID().toString(),
                userId,
                10001 + random.nextDouble() * 90000, // amount between 10001-100000
                Instant.now(),
                randomLocation());

        log.info("SIM [Large] userId: {} | amount: {}", userId, transaction.amount());
        transactionProducer.sendTransaction(transaction);
    }

    private void simulateLocationAnomaly() {
        String userId = randomUser();

        // First transaction — establish location
        Transaction first = new Transaction(
                UUID.randomUUID().toString(),
                userId,
                200 + random.nextDouble() * 500,
                Instant.now(),
                "Delhi");
        transactionProducer.sendTransaction(first);

        // Second transaction — impossible travel immediately after
        Transaction second = new Transaction(
                UUID.randomUUID().toString(),
                userId,
                200 + random.nextDouble() * 500,
                Instant.now(),
                "NewYork");

        log.info("SIM [Location] userId: {} | Delhi -> NewYork", userId);
        transactionProducer.sendTransaction(second);
    }

    private void simulateRapidTransactions() {
        String userId = RAPID_USERS.get(random.nextInt(RAPID_USERS.size()));
        String location = randomLocation();

        log.info("SIM [Rapid] userId: {} | firing 6 transactions", userId);

        // Fire 6 transactions rapidly for the same user
        for (int i = 0; i < 6; i++) {
            Transaction transaction = new Transaction(
                    UUID.randomUUID().toString(),
                    userId,
                    100 + random.nextDouble() * 500,
                    Instant.now(),
                    location);
            transactionProducer.sendTransaction(transaction);
        }
    }

    private String randomUser() {
        return USER_IDS.get(random.nextInt(USER_IDS.size()));
    }

    private String randomLocation() {
        return LOCATIONS.get(random.nextInt(LOCATIONS.size()));
    }
}
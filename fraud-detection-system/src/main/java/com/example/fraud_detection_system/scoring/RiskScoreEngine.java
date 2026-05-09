package com.example.fraud_detection_system.scoring;

import com.example.fraud_detection_system.model.RiskLevel;
import com.example.fraud_detection_system.model.RiskScore;
import com.example.fraud_detection_system.model.Transaction;
import com.example.fraud_detection_system.rules.LargeTransactionRule;
import com.example.fraud_detection_system.rules.LocationRule;
import com.example.fraud_detection_system.rules.VelocityRule;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Component
public class RiskScoreEngine {

    private static final int LARGE_TRANSACTION_SCORE = 30;
    private static final int RAPID_TRANSACTION_SCORE = 20;
    private static final int LOCATION_ANOMALY_SCORE = 50;

    private final LargeTransactionRule largeTransactionRule;
    private final LocationRule locationRule;

    public RiskScoreEngine(LargeTransactionRule largeTransactionRule,
                           LocationRule locationRule) {
        this.largeTransactionRule = largeTransactionRule;
        this.locationRule = locationRule;
    }
    public RiskScore evaluate(Transaction transaction,
                              long velocityCount,
                              String lastLocation,
                              long lastTimestamp) {
        int totalScore = 0;
        List<String> triggeredRules = new ArrayList<>();

        // Rule 1 — Large Transaction
        if (largeTransactionRule.isFraudulent(transaction)) {
            totalScore += LARGE_TRANSACTION_SCORE;
            triggeredRules.add("LARGE_TRANSACTION");
        }

        // Rule 2 — Rapid Transactions
        if (velocityCount >= 5) {
            totalScore += RAPID_TRANSACTION_SCORE;
            triggeredRules.add("RAPID_TRANSACTIONS");
        }

        // Rule 3 — Location Anomaly
        if (lastLocation != null &&
                locationRule.isImpossibleTravel(
                        lastLocation,
                        transaction.location(),
                        lastTimestamp,
                        transaction.timestamp().toEpochMilli())) {
            totalScore += LOCATION_ANOMALY_SCORE;
            triggeredRules.add("LOCATION_ANOMALY");
        }

        return new RiskScore(
                transaction.transactionId(),
                transaction.userId(),
                totalScore,
                RiskLevel.fromScore(totalScore),
                triggeredRules,
                Instant.now()
        );
    }
}
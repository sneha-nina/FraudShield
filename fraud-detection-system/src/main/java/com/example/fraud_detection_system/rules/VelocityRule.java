package com.example.fraud_detection_system.rules;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class VelocityRule {

    private final int maxTransactionCount;

    public VelocityRule(
            @Value("${fraud.rules.velocity-transaction-count}") int maxTransactionCount) {
        this.maxTransactionCount = maxTransactionCount;
    }

    public boolean isFraudulent(long transactionCount) {
        return transactionCount >= maxTransactionCount;
    }
}

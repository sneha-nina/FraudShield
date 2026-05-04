package com.example.fraud_detection_system.rules;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class VelocityRule {
    @Value("${fraud.rules.velocity-transaction-count}")
    private int maxTransactionCount;

    public boolean isFraudulent(long transactionCount) {
        return transactionCount >= maxTransactionCount;
    }
}

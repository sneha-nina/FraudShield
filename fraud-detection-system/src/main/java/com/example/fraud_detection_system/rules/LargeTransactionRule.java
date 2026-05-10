package com.example.fraud_detection_system.rules;

import com.example.fraud_detection_system.model.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class LargeTransactionRule {

    private final double threshold;

    public LargeTransactionRule(
            @Value("${fraud.rules.large-transaction-threshold}") double threshold) {
        this.threshold = threshold;
    }

    public boolean isFraudulent(Transaction transaction) {
        return transaction.amount() > threshold;
    }
}

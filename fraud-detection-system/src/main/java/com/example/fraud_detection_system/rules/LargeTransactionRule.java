package com.example.fraud_detection_system.rules;

import com.example.fraud_detection_system.model.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class LargeTransactionRule {
    @Value("${fraud.rules.large-transaction-threshold}")
    private double threshold;

    public boolean isFraudulent(Transaction transaction) {
        return transaction.amount() > threshold;
    }
}

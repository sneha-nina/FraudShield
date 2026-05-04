package com.example.fraud_detection_system.model;

import java.time.Instant;

public record FraudAlert(
        String alertId,
        String userId,
        AlertType alertType,
        String description,
        Instant timestamp
) {
    public enum AlertType {
        LARGE_TRANSACTION,
        RAPID_TRANSACTIONS,
        LOCATION_ANOMALY
    }
}
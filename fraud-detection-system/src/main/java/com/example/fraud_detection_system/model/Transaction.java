package com.example.fraud_detection_system.model;

import java.time.Instant;

public record Transaction(String transactionId,
                          String userId,
                          double amount,
                          Instant timestamp,
                          String location
)
{
}

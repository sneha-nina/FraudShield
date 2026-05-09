package com.example.fraud_detection_system.model;

import java.time.Instant;
import java.util.List;

public record RiskScore(
        String transactionId,
        String userId,
        int totalScore,
        RiskLevel riskLevel,
        List<String> triggeredRules,
        Instant timestamp
) {}
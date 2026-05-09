package com.example.fraud_detection_system.model;

public enum RiskLevel {
    CLEAN,
    LOW,
    MEDIUM,
    HIGH;

    public static RiskLevel fromScore(int score) {
        if (score == 0)        return CLEAN;
        if (score <= 30)       return LOW;
        if (score <= 60)       return MEDIUM;
        return HIGH;
    }
}
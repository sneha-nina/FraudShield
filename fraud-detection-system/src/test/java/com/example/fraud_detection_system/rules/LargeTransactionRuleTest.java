package com.example.fraud_detection_system.rules;

import com.example.fraud_detection_system.model.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class LargeTransactionRuleTest {

    private LargeTransactionRule rule;

    @BeforeEach
    void setUp() {
        // No Spring needed — just pass threshold directly
        rule = new LargeTransactionRule(10000.0);
    }

    @Test
    void shouldNotFlagTransactionBelowThreshold() {
        Transaction transaction = new Transaction(
                UUID.randomUUID().toString(),
                "user1", 500.0, Instant.now(), "Delhi");

        assertFalse(rule.isFraudulent(transaction));
    }

    @Test
    void shouldNotFlagTransactionEqualToThreshold() {
        Transaction transaction = new Transaction(
                UUID.randomUUID().toString(),
                "user1", 10000.0, Instant.now(), "Delhi");

        assertFalse(rule.isFraudulent(transaction));
    }

    @Test
    void shouldFlagTransactionAboveThreshold() {
        Transaction transaction = new Transaction(
                UUID.randomUUID().toString(),
                "user1", 15000.0, Instant.now(), "Delhi");

        assertTrue(rule.isFraudulent(transaction));
    }
}
package com.example.fraud_detection_system.consumer;

import com.example.fraud_detection_system.model.FraudAlert;
import com.example.fraud_detection_system.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class FraudAlertConsumer {
    private static final Logger log = LoggerFactory.getLogger(FraudAlertConsumer.class);

    @KafkaListener(topics = "fraud-alerts", groupId = "fraud-alert-group")
    public void consumeFraudAlert(FraudAlert alert) {
        log.error("FRAUD ALERT RECEIVED — type: {} | userId: {} | reason: {} | time: {}",
                alert.alertType(),
                alert.userId(),
                alert.description(),
                alert.timestamp());
    }
}

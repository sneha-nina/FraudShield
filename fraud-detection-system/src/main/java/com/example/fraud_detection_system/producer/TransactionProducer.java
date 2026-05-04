package com.example.fraud_detection_system.producer;

import com.example.fraud_detection_system.model.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionProducer {
    private static final String TOPIC = "transactions";

    private final KafkaTemplate<String, Transaction> kafkaTemplate;

    public void sendTransaction(Transaction transaction) {
        kafkaTemplate.send(TOPIC, transaction.userId(), transaction)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send transaction {}: {}",
                                transaction.transactionId(), ex.getMessage());
                    } else {
                        log.info("Transaction sent: {} | partition: {} | offset: {}",
                                transaction.transactionId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}

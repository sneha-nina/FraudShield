package com.example.fraud_detection_system.controller;

import com.example.fraud_detection_system.model.Transaction;
import com.example.fraud_detection_system.producer.TransactionProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/transactions")
@RequiredArgsConstructor
public class TransactionController {
    private final TransactionProducer transactionProducer;

    @PostMapping
    public ResponseEntity<String> submitTransaction(@RequestBody TransactionRequest request) {
        Transaction transaction = new Transaction(
                UUID.randomUUID().toString(),
                request.userId(),
                request.amount(),
                Instant.now(),
                request.location()
        );

        transactionProducer.sendTransaction(transaction);
        log.info("Transaction submitted: {}", transaction.transactionId());

        return ResponseEntity.ok("Transaction submitted: " + transaction.transactionId());
    }
    // Separate request record — don't expose Transaction internals to API consumers
    public record TransactionRequest(
            String userId,
            double amount,
            String location
    ) {}
}

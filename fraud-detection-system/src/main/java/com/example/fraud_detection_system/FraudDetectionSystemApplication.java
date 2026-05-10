package com.example.fraud_detection_system;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableKafkaStreams
@EnableScheduling

public class FraudDetectionSystemApplication {

	public static void main(String[] args) {
		SpringApplication.run(FraudDetectionSystemApplication.class, args);
	}

}

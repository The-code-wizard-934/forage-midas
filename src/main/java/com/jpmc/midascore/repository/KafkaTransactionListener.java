package com.jpmc.midascore.repository;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka listener component.
 * This version is configured to receive a raw String message for debugging.
 */
@Component
public class KafkaTransactionListener {

    /**
     * Listens to the Kafka topic.
     * The parameter is changed to String to capture the raw message
     * and bypass any JSON deserialization errors for now.
     *
     * @param message The raw message received from Kafka
     */
    @KafkaListener(topics = "${midas.kafka.topic}", groupId = "midas-core-consumer")
    public void handleTransaction(String message) {

        // --- SET YOUR BREAKPOINT ON THIS LINE ---
        System.out.println("Received raw message: " + message);

    }
}
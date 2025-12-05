package com.jpmc.midascore.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class KafkaTransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger logger = LoggerFactory.getLogger(KafkaTransactionListener.class);

    public KafkaTransactionListener(UserRepository userRepository,
                                    TransactionRecordRepository transactionRecordRepository,
                                    RestTemplate restTemplate) {

        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${midas.kafka.topic}", groupId = "midas-core")
    public void handleIncomingMessage(String message) {

        try {
            // Convert JSON → Transaction
            Transaction tx = objectMapper.readValue(message, Transaction.class);

            UserRecord sender = userRepository.findById(tx.getSenderId()).orElse(null);
            UserRecord recipient = userRepository.findById(tx.getRecipientId()).orElse(null);

            // Validate users exist
            if (sender == null || recipient == null) {
                logger.warn("Discarded transaction (user not found): {}", tx);
                return;
            }

            // Validate balance
            if (sender.getBalance() < tx.getAmount()) {
                logger.warn("Discarded transaction (insufficient balance): {}", tx);
                return;
            }

            // -------------------------------
            //  CALL INCENTIVE API
            // -------------------------------
            Incentive incentive = restTemplate.postForObject(
                    "http://localhost:8080/incentive",
                    tx,
                    Incentive.class
            );

            float incentiveAmount =
                    (incentive != null) ? incentive.getAmount() : 0f;

            // -------------------------------
            //  UPDATE USER BALANCES
            // -------------------------------
            sender.setBalance(sender.getBalance() - tx.getAmount());
            recipient.setBalance(recipient.getBalance() + tx.getAmount() + incentiveAmount);

            userRepository.save(sender);
            userRepository.save(recipient);

            // -------------------------------
            //  SAVE TRANSACTION RECORD
            // -------------------------------
            TransactionRecord record =
                    new TransactionRecord(sender, recipient, tx.getAmount(), incentiveAmount);

            transactionRecordRepository.save(record);

            logger.info("Processed transaction: {} with incentive {}", tx, incentiveAmount);

        } catch (Exception e) {
            logger.error("FAILED to process transaction message", e);
        }
    }
}

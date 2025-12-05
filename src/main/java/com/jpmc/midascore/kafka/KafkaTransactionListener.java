package com.jpmc.midascore.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(KafkaTransactionListener.class);

    public KafkaTransactionListener(UserRepository userRepository,
                                    TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(topics = "${midas.kafka.topic}", groupId = "midas")
    public void handleIncomingMessage(String message) {
        try {
            Transaction tx = objectMapper.readValue(message, Transaction.class);

            UserRecord sender = userRepository.findById(tx.getSenderId()).orElse(null);
            UserRecord recipient = userRepository.findById(tx.getRecipientId()).orElse(null);

            if (sender == null || recipient == null) {
                logger.warn("Invalid transaction — user not found: {}", tx);
                return;
            }

            if (sender.getBalance() < tx.getAmount()) {
                logger.warn("Invalid transaction — insufficient balance: {}", tx);
                return;
            }

            // update balances
            sender.setBalance(sender.getBalance() - tx.getAmount());
            recipient.setBalance(recipient.getBalance() + tx.getAmount());

            // save updated users
            userRepository.save(sender);
            userRepository.save(recipient);

            // save transaction record
            TransactionRecord record = new TransactionRecord(sender, recipient, tx.getAmount());
            transactionRecordRepository.save(record);

            logger.info("Transaction recorded: {}", tx);

        } catch (Exception ex) {
            logger.error("Failed to process kafka transaction", ex);
        }
    }
}

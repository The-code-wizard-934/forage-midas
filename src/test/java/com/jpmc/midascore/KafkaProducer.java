package com.jpmc.midascore;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${midas.kafka.topic}")
    private String topic;


    public void send(String message) {
        kafkaTemplate.send(topic, message);
    }
}

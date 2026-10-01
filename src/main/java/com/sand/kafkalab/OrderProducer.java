package com.sand.kafkalab;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {
    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(String key, String message) {
        kafkaTemplate.send("orders", key, message)
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("Sending to orders failed (clé={})", key, error);
                    }
                });
    }
}

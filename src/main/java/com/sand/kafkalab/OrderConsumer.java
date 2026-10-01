package com.sand.kafkalab;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    @KafkaListener(topics = "orders", groupId = "demo_group")
    public void consumeA(ConsumerRecord<String, String> record) {
        print("A", record);
    }

    @KafkaListener(topics = "orders", groupId = "demo_group")
    public void consumeB(ConsumerRecord<String, String> record) {
        print("B", record);
    }

    @KafkaListener(topics = "orders", groupId = "demo_group")
    public void consumeC(ConsumerRecord<String, String> record) {
        print("C", record);
    }

    @KafkaListener(topics = "orders", groupId = "demo_group")
    public void consumeD(ConsumerRecord<String, String> record) {
        print("D", record);
    }

    @KafkaListener(topics = "orders", groupId = "demo_group")
    public void consumeE(ConsumerRecord<String, String> record) {
        print("E", record);
    }

    private void print(String consumer, ConsumerRecord<String, String> record) {
        System.out.println("CONSUMER " + consumer
                + " | key: " + record.key()
                + " | value: " + record.value()
                + " | partition: " + record.partition()
                + " | offset: " + record.offset());
    }
}

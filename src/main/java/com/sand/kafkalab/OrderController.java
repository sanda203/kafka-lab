package com.sand.kafkalab;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {
    private final OrderProducer producer;
    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public void create(@RequestParam String key, @RequestBody String message) {
        producer.send(key, message);
    }

}

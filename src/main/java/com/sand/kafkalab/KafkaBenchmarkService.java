package com.sand.kafkalab;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class KafkaBenchmarkService {
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaBenchmarkService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public BenchmarkResult run (int numberOfMessages) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(numberOfMessages);

        AtomicInteger success = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();

        String payload = "x".repeat(100);
        long start = System.nanoTime();


        for (int i = 0; i < numberOfMessages; i++) {
            kafkaTemplate.send("benchmark", "client-" + (i % 1000), payload)
                    .whenComplete((res, e) -> {
                        if (e == null) {
                            success.incrementAndGet();
                        }else  {
                            failed.incrementAndGet();
                        }

                        countDownLatch.countDown();
                    });
        }
        countDownLatch.await();

        long end = System.nanoTime();
        double durationSeconds = (end - start)/1_000_000_000.0;
        double messagesPerSecond = success.get() / durationSeconds;

        return new BenchmarkResult(
                numberOfMessages,
                success.get(),
                failed.get(),
                durationSeconds,
                messagesPerSecond
        );
    }
}
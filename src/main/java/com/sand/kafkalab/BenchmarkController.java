package com.sand.kafkalab;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/benchmark")
public class BenchmarkController {

    private final KafkaBenchmarkService benchmarkService;

    public BenchmarkController(KafkaBenchmarkService benchmarkService) {
        this.benchmarkService = benchmarkService;
    }

    @PostMapping
    public BenchmarkResult run() throws InterruptedException {
        return benchmarkService.run(10_000_000);
    }
}

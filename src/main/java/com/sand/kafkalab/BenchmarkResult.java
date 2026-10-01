package com.sand.kafkalab;

public record BenchmarkResult(int requested,
                              int successful,
                              int failed,
                              double durationSeconds,
                              double messagesPerSecond) {
}

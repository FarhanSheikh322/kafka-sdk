package io.github.enterprise.kafka.producer;
public record DeliveryResult(String topic, int partition, long offset, long timestamp) {}

package io.github.enterprise.kafka.consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
public record KafkaEvent<T>(T value, String key, String topic, int partition, long offset, ConsumerRecord<String,byte[]> raw) {}

package io.github.enterprise.kafka.producer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
public interface KafkaPublisher extends AutoCloseable {
 <T> DeliveryResult publish(String topic, T value);
 <T> DeliveryResult publish(String topic, String key, T value);
 <T> CompletableFuture<DeliveryResult> publishAsync(String topic, String key, T value);
 default <T> CompletableFuture<DeliveryResult> publishAsync(String topic, T value) { return publishAsync(topic, null, value); }
 <T> List<DeliveryResult> publishBatch(String topic, List<T> values);
 void flush();
 void close();
}

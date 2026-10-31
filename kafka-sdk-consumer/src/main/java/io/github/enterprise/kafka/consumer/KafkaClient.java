package io.github.enterprise.kafka.consumer;
import java.util.function.Consumer;
public interface KafkaClient extends AutoCloseable {
 <T> Subscription subscribe(String topic, String groupId, Class<T> type, Consumer<KafkaEvent<T>> handler);
 default <T> Subscription subscribe(String topic, Class<T> type, Consumer<T> handler) { return subscribe(topic,"enterprise-kafka-sdk",type,event->handler.accept(event.value())); }
 void close();
 interface Subscription extends AutoCloseable { void close(); }
}

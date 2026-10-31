package io.github.enterprise.kafka.consumer;
import io.github.enterprise.kafka.core.KafkaSettings;
import io.github.enterprise.kafka.serialization.JsonCodec;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
public final class DefaultKafkaClient implements KafkaClient {
 private final KafkaSettings settings;
 private final JsonCodec codec;
 private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();
 private final AtomicBoolean closed = new AtomicBoolean();
 public DefaultKafkaClient(KafkaSettings settings, JsonCodec codec) { this.settings=Objects.requireNonNull(settings); this.codec=Objects.requireNonNull(codec); }
 public <T> Subscription subscribe(String topic,String groupId,Class<T> type,Consumer<KafkaEvent<T>> handler) {
  if(closed.get()) throw new IllegalStateException("Client closed");
  Objects.requireNonNull(topic); Objects.requireNonNull(groupId); Objects.requireNonNull(type); Objects.requireNonNull(handler);
  Worker<T> worker=new Worker<>(topic,groupId,type,handler); subscriptions.add(worker); worker.start(); return worker;
 }
 public void close() { if(closed.compareAndSet(false,true)) { for(Subscription s: subscriptions) s.close(); subscriptions.clear(); } }
 private final class Worker<T> implements Subscription,Runnable {
  private final String topic; private final Class<T> type; private final Consumer<KafkaEvent<T>> handler; private final KafkaConsumer<String,byte[]> consumer; private final Thread thread; private final AtomicBoolean running=new AtomicBoolean(true);
  Worker(String topic,String groupId,Class<T> type,Consumer<KafkaEvent<T>> handler) {
   this.topic=topic; this.type=type; this.handler=handler;
   Properties p=new Properties(); p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,settings.bootstrapServers()); p.put(ConsumerConfig.GROUP_ID_CONFIG,groupId); p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class); p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,ByteArrayDeserializer.class); p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false); p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
   consumer=new KafkaConsumer<>(p); thread=ConsumerThreads.unstarted("kafka-sdk-consumer-"+groupId,this);
  }
  void start() { thread.start(); }
  public void run() {
   try { consumer.subscribe(List.of(topic)); while(running.get()) { ConsumerRecords<String,byte[]> records=consumer.poll(Duration.ofMillis(250)); for(ConsumerRecord<String,byte[]> record:records) { if(!running.get()) break; T value=codec.decode(record.value(),type); handler.accept(new KafkaEvent<>(value,record.key(),record.topic(),record.partition(),record.offset(),record)); consumer.commitSync(Map.of(new org.apache.kafka.common.TopicPartition(record.topic(),record.partition()),new OffsetAndMetadata(record.offset()+1))); } } }
   catch(WakeupException e) { if(running.get()) throw e; }
   catch(RuntimeException e) { if(running.get()) { System.err.println("Kafka consumer stopped after processing failure: "+e.getClass().getSimpleName()); } }
   finally { consumer.close(); subscriptions.remove(this); }
  }
  public void close() { if(running.compareAndSet(true,false)) consumer.wakeup(); if(Thread.currentThread()!=thread) { try { thread.join(settings.timeout().toMillis()); if(thread.isAlive()) throw new IllegalStateException("Consumer did not stop within timeout"); } catch(InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException("Interrupted waiting for consumer shutdown",e); } } }
 }
}

package io.github.enterprise.kafka.producer;
import io.github.enterprise.kafka.core.KafkaSettings;
import io.github.enterprise.kafka.serialization.JsonCodec;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
public final class DefaultKafkaPublisher implements KafkaPublisher {
 private final Producer<String,byte[]> producer;
 private final JsonCodec codec;
 private final KafkaSettings settings;
 private final AtomicBoolean closed = new AtomicBoolean();
 public DefaultKafkaPublisher(KafkaSettings settings, JsonCodec codec) { this(settings, codec, Map.of()); }
 public DefaultKafkaPublisher(KafkaSettings settings, JsonCodec codec, Map<String,Object> overrides) {
  this.settings=Objects.requireNonNull(settings); this.codec=Objects.requireNonNull(codec);
  Properties p=new Properties(); p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.bootstrapServers()); p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class); p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,ByteArraySerializer.class); p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true); p.put(ProducerConfig.ACKS_CONFIG,"all"); p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,settings.timeout().toMillis()); p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,(int)Math.min(Integer.MAX_VALUE,Math.max(120000L,settings.timeout().toMillis()))); p.putAll(overrides);
  producer=new KafkaProducer<>(p);
 }
 DefaultKafkaPublisher(KafkaSettings settings, JsonCodec codec, Producer<String,byte[]> producer) { this.settings=Objects.requireNonNull(settings); this.codec=Objects.requireNonNull(codec); this.producer=Objects.requireNonNull(producer); }
 public <T> DeliveryResult publish(String topic,T value) { return publish(topic,null,value); }
 public <T> DeliveryResult publish(String topic,String key,T value) { try { return publishAsync(topic,key,value).get(settings.timeout().toMillis(),TimeUnit.MILLISECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new PublishException("Interrupted while publishing",e); } catch (ExecutionException|TimeoutException e) { throw new PublishException("Publish failed: "+topic,e); } }
 public <T> CompletableFuture<DeliveryResult> publishAsync(String topic,String key,T value) {
  if (closed.get()) return CompletableFuture.failedFuture(new IllegalStateException("Publisher closed"));
  Objects.requireNonNull(topic); Objects.requireNonNull(value);
  CompletableFuture<DeliveryResult> future=new CompletableFuture<>();
  try { producer.send(new ProducerRecord<>(topic,key,codec.encode(value)),(metadata,error)-> { if(error!=null) future.completeExceptionally(new PublishException("Publish failed: "+topic,error)); else future.complete(new DeliveryResult(metadata.topic(),metadata.partition(),metadata.offset(),metadata.timestamp())); }); }
  catch (Exception e) { future.completeExceptionally(new PublishException("Publish failed: "+topic,e)); }
  return future;
 }
 public <T> List<DeliveryResult> publishBatch(String topic,List<T> values) { Objects.requireNonNull(values); List<CompletableFuture<DeliveryResult>> futures=values.stream().map(v->publishAsync(topic,v)).toList(); return futures.stream().map(CompletableFuture::join).toList(); }
 public void flush() { producer.flush(); }
 public void close() { if(closed.compareAndSet(false,true)) producer.close(settings.timeout()); }
}

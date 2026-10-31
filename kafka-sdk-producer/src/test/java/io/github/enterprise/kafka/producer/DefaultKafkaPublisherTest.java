package io.github.enterprise.kafka.producer;
import io.github.enterprise.kafka.core.KafkaSettings;
import io.github.enterprise.kafka.serialization.JsonCodec;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
class DefaultKafkaPublisherTest {
 @Test void synchronousAndBatchDelivery() {
  MockProducer<String,byte[]> mock=new MockProducer<>(true,null,new StringSerializer(),new ByteArraySerializer());
  try(DefaultKafkaPublisher publisher=new DefaultKafkaPublisher(KafkaSettings.of("localhost:9092"),new JsonCodec(),mock)) {
   assertEquals(0,publisher.publish("payments","key",new Event("a")).offset());
   assertEquals(2,publisher.publishBatch("payments",List.of(new Event("b"),new Event("c"))).size());
   assertEquals("key",mock.history().get(0).key());
   assertEquals(3,mock.history().size());
  }
 }
 record Event(String id) {}
}

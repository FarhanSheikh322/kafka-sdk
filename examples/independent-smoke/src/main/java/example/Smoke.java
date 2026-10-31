package example;
import io.github.enterprise.kafka.producer.KafkaPublisher;
import io.github.enterprise.kafka.consumer.KafkaClient;
import io.github.enterprise.kafka.spring.KafkaSdkAutoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;
public class Smoke {
 @Configuration @EnableAutoConfiguration static class App {}
 public static void main(String[] args) {
  try (var context = SpringApplication.run(App.class, args)) {
   context.getBean(KafkaPublisher.class);
   context.getBean(KafkaClient.class);
   if(!context.containsBean("kafkaPublisher")) throw new IllegalStateException("publisher missing");
   System.out.println("SDK_BEANS_OK");
  }
 }
}

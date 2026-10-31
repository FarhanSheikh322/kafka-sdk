package io.github.enterprise.kafka.spring;
import io.github.enterprise.kafka.core.KafkaSettings;
import io.github.enterprise.kafka.serialization.JsonCodec;
import io.github.enterprise.kafka.producer.*;
import io.github.enterprise.kafka.consumer.*;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
@AutoConfiguration
@EnableConfigurationProperties(KafkaSdkProperties.class)
@ConditionalOnProperty(prefix="enterprise.kafka",name="enabled",havingValue="true",matchIfMissing=true)
public class KafkaSdkAutoConfiguration {
 @Bean @ConditionalOnMissingBean public KafkaSettings kafkaSettings(KafkaSdkProperties p){return KafkaSettings.of(p.getBootstrapServers());}
 @Bean @ConditionalOnMissingBean public JsonCodec kafkaJsonCodec(){return new JsonCodec();}
 @Bean @ConditionalOnMissingBean public KafkaPublisher kafkaPublisher(KafkaSettings s,JsonCodec c){return new DefaultKafkaPublisher(s,c);}
 @Bean @ConditionalOnMissingBean public KafkaClient kafkaClient(KafkaSettings s,JsonCodec c){return new DefaultKafkaClient(s,c);}
}

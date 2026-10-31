package io.github.enterprise.kafka.spring;
import org.springframework.boot.context.properties.ConfigurationProperties;
@ConfigurationProperties(prefix="enterprise.kafka")
public class KafkaSdkProperties {
 private boolean enabled=true; private String bootstrapServers="localhost:9092"; private String groupId="enterprise-kafka-sdk";
 public boolean isEnabled(){return enabled;} public void setEnabled(boolean enabled){this.enabled=enabled;}
 public String getBootstrapServers(){return bootstrapServers;} public void setBootstrapServers(String value){bootstrapServers=value;}
 public String getGroupId(){return groupId;} public void setGroupId(String value){groupId=value;}
}

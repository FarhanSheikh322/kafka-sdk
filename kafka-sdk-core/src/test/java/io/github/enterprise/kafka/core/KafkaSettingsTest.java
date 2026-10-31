package io.github.enterprise.kafka.core;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class KafkaSettingsTest { @Test void validates(){ assertThrows(IllegalArgumentException.class,()->KafkaSettings.of(" ")); assertEquals("localhost:9092",KafkaSettings.of("localhost:9092").bootstrapServers()); } }

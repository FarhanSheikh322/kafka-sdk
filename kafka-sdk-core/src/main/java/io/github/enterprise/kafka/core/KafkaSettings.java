package io.github.enterprise.kafka.core;
import java.time.Duration;
import java.util.Objects;
public record KafkaSettings(String bootstrapServers, Duration timeout) {
 public KafkaSettings { if (bootstrapServers == null || bootstrapServers.isBlank()) throw new IllegalArgumentException("bootstrapServers is required"); Objects.requireNonNull(timeout); if (timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("timeout must be positive"); }
 public static KafkaSettings of(String bootstrapServers) { return new KafkaSettings(bootstrapServers, Duration.ofSeconds(30)); }
}

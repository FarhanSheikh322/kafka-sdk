package io.github.enterprise.kafka.core;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
public record EventEnvelope<T>(UUID eventId, String eventType, int eventVersion, String source, Instant timestamp, UUID correlationId, T payload) {
 public EventEnvelope { Objects.requireNonNull(eventId); Objects.requireNonNull(eventType); Objects.requireNonNull(source); Objects.requireNonNull(timestamp); Objects.requireNonNull(payload); if (eventVersion < 1) throw new IllegalArgumentException("eventVersion must be positive"); }
 public static <T> EventEnvelope<T> create(String eventType, int version, String source, T payload) { return new EventEnvelope<>(UUID.randomUUID(), eventType, version, source, Instant.now(), UUID.randomUUID(), payload); }
}

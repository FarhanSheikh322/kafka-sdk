package io.github.enterprise.kafka.retry;
import java.time.Duration;
import java.util.Objects;
public record RetryPolicy(int maxAttempts, Duration initialDelay, double multiplier, Duration maxDelay) {
 public RetryPolicy { if(maxAttempts<1) throw new IllegalArgumentException("maxAttempts must be positive"); Objects.requireNonNull(initialDelay); Objects.requireNonNull(maxDelay); if(initialDelay.isNegative() || maxDelay.isNegative() || multiplier<1 || !Double.isFinite(multiplier)) throw new IllegalArgumentException("Invalid retry delay"); }
 public Duration delayFor(int attempt) { if(attempt<1) throw new IllegalArgumentException("attempt must be positive"); double millis=initialDelay.toMillis()*Math.pow(multiplier,attempt-1); return Duration.ofMillis((long)Math.min(maxDelay.toMillis(),millis)); }
}

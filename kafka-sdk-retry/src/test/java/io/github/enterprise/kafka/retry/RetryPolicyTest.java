package io.github.enterprise.kafka.retry;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;
class RetryPolicyTest { @Test void cappedBackoff(){RetryPolicy p=new RetryPolicy(5,Duration.ofMillis(100),2,Duration.ofMillis(250)); assertEquals(Duration.ofMillis(100),p.delayFor(1)); assertEquals(Duration.ofMillis(200),p.delayFor(2)); assertEquals(Duration.ofMillis(250),p.delayFor(3));} }

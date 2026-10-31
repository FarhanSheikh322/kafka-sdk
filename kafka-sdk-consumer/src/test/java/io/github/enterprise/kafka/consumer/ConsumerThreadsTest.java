package io.github.enterprise.kafka.consumer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerThreadsTest {
    @Test
    void createsRunnableThreadWithExpectedRuntimeKind() throws Exception {
        Thread thread = ConsumerThreads.unstarted("kafka-sdk-test", () -> {});
        assertEquals("kafka-sdk-test", thread.getName());
        boolean virtualAvailable;
        try {
            Thread.class.getMethod("ofVirtual");
            virtualAvailable = true;
        } catch (NoSuchMethodException e) {
            virtualAvailable = false;
        }
        if (virtualAvailable) {
            assertTrue((boolean) Thread.class.getMethod("isVirtual").invoke(thread));
        } else {
            assertTrue(thread.isDaemon());
        }
        thread.start();
        thread.join(1000);
        assertFalse(thread.isAlive());
    }
}

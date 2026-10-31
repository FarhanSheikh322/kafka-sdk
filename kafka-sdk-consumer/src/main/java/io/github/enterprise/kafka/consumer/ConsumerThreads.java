package io.github.enterprise.kafka.consumer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

/** Uses virtual threads on Java 21+, while retaining the Java 17 binary baseline. */
final class ConsumerThreads {
    private ConsumerThreads() {}

    static Thread unstarted(String name, Runnable task) {
        Objects.requireNonNull(name);
        Objects.requireNonNull(task);
        try {
            Object builder = Thread.class.getMethod("ofVirtual").invoke(null);
            Class<?> builderType = Class.forName("java.lang.Thread$Builder");
            Method named = builderType.getMethod("name", String.class);
            Object namedBuilder = named.invoke(builder, name);
            return (Thread) builderType.getMethod("unstarted", Runnable.class)
                    .invoke(namedBuilder, task);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Virtual thread builder is unavailable", e);
        } catch (NoSuchMethodException e) {
            Thread thread = new Thread(task, name);
            thread.setDaemon(true);
            return thread;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Cannot create Kafka consumer thread", e);
        }
    }
}

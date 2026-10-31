# Enterprise Kafka SDK (development preview)

Java 17+ Maven multi-module SDK. This repository is **not production ready**.

## Build

```sh
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
mvn clean verify
```

## Plain Java

```java
KafkaSettings settings = KafkaSettings.of("localhost:9092");
JsonCodec codec = new JsonCodec();
try (KafkaPublisher publisher = new DefaultKafkaPublisher(settings, codec);
     KafkaClient client = new DefaultKafkaClient(settings, codec)) {
  client.subscribe("payments", "payment-service", PaymentEvent.class, event -> process(event.value()));
  publisher.publish("payments", "payment-123", new PaymentEvent("payment-123"));
}
```

Consumer callbacks execute on their dedicated poll thread (virtual on Java 21+, daemon platform thread on Java 17). Offsets commit after each successful callback. A callback or deserialization failure stops that subscription, leaves the failed offset uncommitted, and requires an external supervisor to restart it. Callbacks must finish within Kafka's max poll interval.

## Spring Boot

Add `kafka-sdk-spring-boot-starter:0.1.0` and configure `enterprise.kafka.bootstrap-servers`. Beans are conditional on missing user beans.

See `docs/architecture.md` for scope and limitations.

## Independent dependency smoke test

```sh
mvn clean install
mvn -f examples/independent-smoke/pom.xml compile exec:java
```

This validates dependency resolution and Spring bean startup without a Kafka broker. It does not validate delivery.

### Maven dependency

```xml
<dependencyManagement><dependencies>
  <dependency><groupId>io.github.enterprise.kafka</groupId><artifactId>kafka-sdk-bom</artifactId><version>0.1.0</version><type>pom</type><scope>import</scope></dependency>
</dependencies></dependencyManagement>
<dependencies>
  <dependency><groupId>io.github.enterprise.kafka</groupId><artifactId>kafka-sdk-spring-boot-starter</artifactId></dependency>
</dependencies>
```

```yaml
enterprise:
  kafka:
    bootstrap-servers: localhost:9092
```

The standalone example in `examples/independent-smoke` is built outside the reactor and resolves these artifacts from a Maven repository after `mvn install`.

# Architecture and status

Dependency flow: core → serialization → producer/consumer → Spring Boot starter. Retry policy depends only on core. The BOM aligns SDK modules. Kafka producer uses one thread-safe client and Kafka's own bounded buffering. Each consumer subscription owns one KafkaConsumer and one poll thread. On Java 21+ this is a virtual thread; on Java 17 it is a daemon platform thread. Virtual threads do not change KafkaConsumer thread ownership or the one-consumer-per-subscription model. Failed processing halts the worker before offset commit.

Implemented: validated bootstrap/timeout settings, JSON codec, event envelope, sync/async/batch producer, idempotence defaults, typed programmatic consumer with post-processing commits, conditional Spring Boot beans, retry delay calculation.

Not implemented: annotation listeners, DLT, transaction coordination, security secret providers, metrics/tracing, admin, schema registry, outbox, streams, integration examples, performance tests. The retry policy is not yet wired into consumption. No external side-effect exactly-once claim is made.

Kafka 4.4.0, Spring Boot 3.5.16, Jackson 2.22.2. Java 17 baseline.

# Testing status

Run `mvn clean verify`, `mvn test`, and `mvn -DskipTests=false verify` with Java 17. The current tests exercise validation, JSON round trips, retry delay, and a Kafka MockProducer. Docker-backed integration and end-to-end tests have not been implemented. This CI pipeline verifies compilation and an independently built Spring bean smoke test; it does not yet meet the full integration, coverage, security, or release gates requested for production use.

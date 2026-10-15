kafka-streams
===============

Short description
-----------------
A small Spring Boot Kafka Streams application that:
- Reads String messages (String key) from the "input" topic.
- Groups messages by key into session windows (10s inactivity gap, 5s grace) and aggregates them into ArrayList<String> per session.
- Writes aggregated lists as JSON to the "output" topic using a Jackson serde.
- Defines "input" and "output" topics via NewTopic beans.

Key components
- com.ani.kafkastreams.consumer.SampleConsumer: stream topology and aggregation.
- com.ani.kafkastreams.config.KafkaConfig: topic beans and Kafka Streams configuration.
- com.ani.kafkastreams.serdes.CustomSerdes: Jackson Json Serde for ArrayList<String>.

Prerequisites
- Java 17
- Maven (wrapper included: ./mvnw)
- Docker (optional, for local Kafka broker)

Running locally
---------------
1. Start a local Kafka broker (recommended):
   docker-compose up -d
   The included docker-compose.yml exposes Kafka on localhost:9092.

2. Build the project:
   ./mvnw clean package

3. Run the application:
   - Using the Maven Spring Boot plugin (recommended for development):
     ./mvnw spring-boot:run -- -Dspring-boot.run.arguments="--spring.kafka.streams.auto-startup=true"
   - Or run the packaged jar and pass the property to auto-start Streams:
     java -jar target/kafka-streams-0.0.1-SNAPSHOT.jar --spring.kafka.streams.auto-startup=true

Notes:
- By default spring.kafka.streams.auto-startup is false (see src/main/resources/application.properties). Pass --spring.kafka.streams.auto-startup=true or change that property to enable automatic stream startup.
- Topics "input" and "output" are created at application startup by the NewTopic beans in KafkaConfig.

Testing
-------
Run the test suite:
  ./mvnw test

Run a single test class:
  ./mvnw -Dtest=KafkaStreamsApplicationTests test

What tests exist now:
- A Spring Boot context smoke test: src/test/java/com/ani/kafkastreams/KafkaStreamsApplicationTests.java (verifies the app context loads).

Recommended tests to add:
- Topology/unit tests using Kafka Streams TopologyTestDriver for deterministic testing of the stream logic.
- Integration tests using spring-kafka-test or Testcontainers to run against an embedded or containerized Kafka broker.

Examples for developers
- To enable streams only when Kafka is available, run docker-compose up -d and then start the app with auto-startup=true as shown above.
- Use spring-boot-starter-kafka-test to mock or spin up test brokers for integration tests.

Further notes
- Configuration: change bootstrap servers in src/main/resources/application.properties (spring.kafka.bootstrap-servers).
- Contributions and PRs: add topology tests and integration tests as separate commits.


kafka-streams
===============

What this project does
----------------------
A simple Spring Boot Kafka Streams application that:
- Reads String messages keyed by String from the "input" topic.
- Groups messages by key into session windows (10s inactivity gap, 5s grace) and aggregates messages into ArrayList<String> for each session.
- Writes aggregated lists to the "output" topic using a Jackson JSON serde.
- Creates topics "input" and "output" at startup (via NewTopic beans).

Key components
- com.ani.kafkastreams.consumer.SampleConsumer: stream definition and aggregation.
- com.ani.kafkastreams.config.KafkaConfig: topics and Streams configuration (application id, bootstrap servers, timestamp extractor).
- com.ani.kafkastreams.serdes.CustomSerdes: Jackson Json Serde for ArrayList<String>.

Running locally
---------------
1. Start Kafka (recommended):
   docker-compose up -d
   (docker-compose.yml included config for a single Kafka broker mapped to localhost:9092)

2. Build and run the app:
   ./mvnw clean package
   ./mvnw spring-boot:run
   or run the generated jar: java -jar target/kafka-streams-0.0.1-SNAPSHOT.jar

3. The app reads from and writes to topics named `input` and `output`. Change bootstrap servers in src/main/resources/application.properties (spring.kafka.bootstrap-servers).

Testing
-------
- Unit tests: mvn test
- Current test suite includes a Spring context smoke test (src/test/java/.../KafkaStreamsApplicationTests.java) that verifies the application context loads.
- Test dependencies (spring-boot-starter-kafka-test) are present and can be used to add integration tests (embedded Kafka) or use Kafka Streams TopologyTestDriver for topology/unit testing.

Notes / Next steps
- Add topology/unit tests with TopologyTestDriver for deterministic stream behavior.
- Add integration tests using spring-kafka-test to run against an embedded or testcontainer Kafka broker.


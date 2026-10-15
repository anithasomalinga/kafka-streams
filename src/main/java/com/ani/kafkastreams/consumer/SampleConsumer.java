package com.ani.kafkastreams.consumer;

import com.ani.kafkastreams.config.KafkaConfig;
import com.ani.kafkastreams.serdes.CustomSerdes;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;

@Slf4j
@Component
public class SampleConsumer {

    @Bean
    KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
        log.info("Inside Kafka Streams consumer");
        KStream<String, String> stream = streamsBuilder.stream("input", Consumed.with(Serdes.String(), Serdes.String()));
        Duration windowSize = Duration.ofSeconds(10);
        Duration gracePeriod = Duration.ofSeconds(5);
        SessionWindows sessionWindow = SessionWindows.ofInactivityGapAndGrace(windowSize, gracePeriod);
        stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(sessionWindow)
                .aggregate(ArrayList::new,
                        (key, value, aggregate1) -> {aggregate1.add(value); return aggregate1; },
                        (key, aggregate1, aggregate2) -> {
                            aggregate1.addAll(aggregate2); return aggregate1;},
                        Materialized.with(Serdes.String(), CustomSerdes.MessageList()))
                .toStream()
                .map((key, value) -> new KeyValue<>(key.key(), value))
                .peek((key, value) -> log.info("Outgoing record -> key: {}, value: {}", key, value))
                .to("output", Produced.with(Serdes.String(), CustomSerdes.MessageList()));
        return stream;
    }
}

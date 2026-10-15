package com.ani.kafkastreams.config;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

@Configuration
public class KafkaStreamsActuatorConfig {

    @Bean
    public HealthIndicator kafkaStreamsHealthIndicator(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        return () -> {
            KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();

            // If the streams object hasn't been initialized yet
            if (kafkaStreams == null) {
                return Health.down().withDetail("status", "Initializing / Not Started").build();
            }

            KafkaStreams.State state = kafkaStreams.state();

            // If the state is RUNNING or REBALANCING, consider it healthy
            if (state.isRunningOrRebalancing()) {
                return Health.up()
                        .withDetail("state", state.toString())
                        .withDetail("applicationId", streamsBuilderFactoryBean.getStreamsConfiguration().getProperty("application.id"))
                        .build();
            } else {
                // If it is ERROR, NOT_RUNNING, or PENDING_SHUTDOWN, report it DOWN
                return Health.down()
                        .withDetail("state", state.toString())
                        .withDetail("reason", "Kafka Streams thread is not running.")
                        .build();
            }
        };
    }
}
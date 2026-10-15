package com.ani.kafkastreams.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.util.ArrayList;

public final class CustomSerdes {
    public static Serde<ArrayList<String>> MessageList() {
        return new JacksonJsonSerde<>(ArrayList.class);
    }
}

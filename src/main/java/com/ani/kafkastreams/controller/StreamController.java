package com.ani.kafkastreams.controller;

import com.ani.kafkastreams.model.Message;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

@Slf4j
@RestController
@RequestMapping("/api/data")
public class StreamController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    public StreamController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public String postDataToInputTopic() throws JsonProcessingException {
        String keyChars = "abc";
        String valueChars = "12345";
        Random random = new Random();
        for (int i = 0; i < 15; i++) {
            log.info("Key: {}", keyChars.charAt(random.nextInt(keyChars.length())));
            log.info("Value: {}", mapper.writeValueAsString(new Message(Character.toString(valueChars.charAt(random.nextInt(valueChars.length()))))));
            Message message = new Message(Character.toString(valueChars.charAt(random.nextInt(valueChars.length()))));
            kafkaTemplate.send("input", Character.toString(keyChars.charAt(random.nextInt(keyChars.length()))), mapper.writeValueAsString(message));
        }
        return "Data sent successfully to input topic!!!";
    }

}

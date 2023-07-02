package com.example.KafkaBasic.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaConsumer {

    private static final String TOPIC_NAME = "topic5";

    private final ObjectMapper objectMapper;

    @KafkaListener(topics = TOPIC_NAME)
    public void listenMessage(String jsonMessage) {
        try {
            MyMessage message = objectMapper.readValue(jsonMessage, MyMessage.class);
            System.out.println(">>>" + message.getName() + ", " + message.getMessage());
        } catch (Exception e) {
            e.printStackTrace();;
        }
    }
}

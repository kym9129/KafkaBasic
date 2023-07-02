package com.example.KafkaBasic.producer;

import com.example.KafkaBasic.producer.KafkaProduceService;
import com.example.KafkaBasic.producer.MyMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class ProducerController {

    private final KafkaProduceService kafkaProduceService;

    @GetMapping("/publish") // 원래는 Post가 적합하지만 실습하기 편하게 Get 사용
    public String publish(@RequestParam String message) {
        kafkaProduceService.send(message);
        return "published a message : " + message;
    }

    @GetMapping("/publish2")
    public String publishWithCallback(@RequestParam String message) {
        kafkaProduceService.sendWithCallback(message);
        return "published a message with callback : " + message;
    }

    // http://localhost:8080/publish3?name=bori&message=meow
    @GetMapping("/publish3")
    public String publishJson(MyMessage message) {
        kafkaProduceService.sendJson(message);
        return "published Json Message : " + message.getName() + ", " + message.getMessage();
    }
}

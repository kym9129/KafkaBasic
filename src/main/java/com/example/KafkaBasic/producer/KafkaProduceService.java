package com.example.KafkaBasic.producer;

import com.example.KafkaBasic.producer.MyMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
@RequiredArgsConstructor
public class KafkaProduceService {

    private static final String TOPIC_NAME = "topic5";

    // bean으로 등록해둔 카프카 템플릿을 이용하여 메시지를 발행
    private final KafkaTemplate<String, String> kafkaTemplate; // value type : string
    private final KafkaTemplate<String, MyMessage> newKafkaTemplate; // value type : JSON

    public void send(String message) {
        kafkaTemplate.send(TOPIC_NAME, message);
    }

    public void sendJson(MyMessage message) {
        newKafkaTemplate.send(TOPIC_NAME, message);
    }

    public void sendWithCallback(String message) {
        // 스프링 카프카에서 message 발행 결과는 ListenableFuture를 사용
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(TOPIC_NAME, message);

        // add callback을 이용하여 브로커 전송 결과를 비동기로 확인할 수 있음
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Failed " + message + " due to : " + ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Sent " + message + " offset:" + result.getRecordMetadata().offset());
            }
        });
    }

}

package com.example.KafkaBasic.producer;

import com.example.KafkaBasic.producer.MyMessage;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * 카프카 접속을 위한 설정 및 스프링 카프카가 제공하는 카프카 템플릿을 사용하기 위한 설정
 */
@Configuration
public class KafkaProducerConfig {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";

    // 프로듀서 팩토리를 통해 카프카 템플릿을 생성
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    // 메시지 발송 모듈에서는 카프카 템플릿을 이용해서 아주 쉽게 메시지를 발행할 수 있음
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // 실무에서 사용 - DTO를 json으로 변환하여 메시지 보내는 설정
    @Bean
    public ProducerFactory<String, MyMessage> newProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class); // Value를 JsonSerializer로 적용
        // ** jackson꺼 말고 org.springframework.kafka.support.serializer.JsonSerializer 이걸 import 해야 함 **

        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, MyMessage> newKafkaTemplate() {
        return new KafkaTemplate<>(newProducerFactory());
    }
}

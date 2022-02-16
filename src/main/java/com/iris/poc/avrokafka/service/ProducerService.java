package com.iris.poc.avrokafka.service;

import com.iris.poc.avrokafka.avro.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ProducerService {

    @Autowired
    private KafkaTemplate<String, ?> kafkaTemplate;

    @Value("${kafka.topic}")
    private String topic;

    public void send(Object payload) {
        log.info("sending payload='{}' to topic='{}'", payload.toString(), topic);
        kafkaTemplate.send(MessageBuilder.withPayload(payload).setHeader(KafkaHeaders.TOPIC, topic).build());
    }
}

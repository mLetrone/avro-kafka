package com.iris.poc.avrokafka.service;

import com.iris.poc.avrokafka.avro.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;


@Slf4j
public class ConsumerService {

    @KafkaListener(topics = "${kafka.topic}")
    public void receive(@Payload Order order) {
        log.info("received {}", order.toString());
    }

}

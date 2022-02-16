package com.iris.poc.avrokafka.controller;

import com.iris.poc.avrokafka.avro.model.Order;
import com.iris.poc.avrokafka.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private ProducerService producerService;

    @PostMapping("/publish")
    public void sendMessageToKafkaTopic(@RequestBody Order order) {
        producerService.send(order);
    }
}

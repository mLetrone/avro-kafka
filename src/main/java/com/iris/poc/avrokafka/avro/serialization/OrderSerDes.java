package com.iris.poc.avrokafka.avro.serialization;

import com.iris.poc.avrokafka.avro.model.Order;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class OrderSerDes implements Serde<Order> {
    private final OrderDeserializer deserializer = new OrderDeserializer();
    private final OrderSerializer serializer = new OrderSerializer();

    @Override
    public Serializer<Order> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Order> deserializer() {
        return deserializer;
    }
}

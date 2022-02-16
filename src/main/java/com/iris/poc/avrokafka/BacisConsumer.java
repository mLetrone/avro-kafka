package com.iris.poc.avrokafka;

import java.io.IOException;
import java.util.Properties;

import com.iris.poc.avrokafka.avro.model.Order;
import com.iris.poc.avrokafka.avro.serialization.OrderDeserializer;
import com.iris.poc.avrokafka.avro.serialization.OrderSerDes;
import com.iris.poc.avrokafka.avro.serialization.OrderSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

public class BacisConsumer {
    public static void main(String[] args) throws IOException {
        final Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        OrderSerDes orderSerDes = new OrderSerDes();
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Order> kStream = builder.stream("test-avro",
                Consumed.with(
                        Serdes.String(),
                        orderSerDes));

        kStream.peek((key, value) -> System.out.println("Incoming record - key " +key +" value " + value));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        kafkaStreams.start();
    }
}

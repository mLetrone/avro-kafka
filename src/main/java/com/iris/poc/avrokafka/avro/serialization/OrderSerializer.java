package com.iris.poc.avrokafka.avro.serialization;

import com.iris.poc.avrokafka.avro.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.util.Map;

@Slf4j
public class OrderSerializer implements Serializer<Order> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public byte[] serialize(String s, Order order) {
        byte[] bytes = null;
        try {
            if (order != null) {
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(stream, null);
                DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(order.getSchema());
                datumWriter.write(order, binaryEncoder);
                binaryEncoder.flush();
                stream.close();
                bytes = stream.toByteArray();
            }
        } catch (Exception e) {
            log.error("Unable to serialize payload ", e);
        }
        return bytes;
    }

    @Override
    public void close() {
        // do nothing
    }
}

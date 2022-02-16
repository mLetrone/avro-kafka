package com.iris.poc.avrokafka.avro.serialization;

import com.iris.poc.avrokafka.avro.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@Slf4j
public class OrderDeserializer implements Deserializer<Order>{

    @Override
    public void configure(Map configs, boolean isKey) {
        // do nothing
    }

    @Override
    public Order deserialize(String s, byte[] bytes) {
        Order returnObject = null;
        try {

            if (bytes != null) {
                DatumReader<GenericRecord> datumReader = new SpecificDatumReader<GenericRecord>(Order.getClassSchema());
                Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
                returnObject = (Order) datumReader.read(null, decoder);
                log.info("deserialized data='{}'", returnObject.toString());
            }
        } catch (Exception e) {
            log.error("Unable to Deserialize bytes[] ", e);
        }
        return returnObject;
    }

    @Override
    public void close() {
        // do nothing
    }
}

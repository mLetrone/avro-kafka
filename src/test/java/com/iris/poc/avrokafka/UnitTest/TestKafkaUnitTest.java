package com.iris.poc.avrokafka.UnitTest;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TestKafkaUnitTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Long> inputTopic;
    private TestOutputTopic<String, Double> outputTopic;
    private KeyValueStore<String, Long> store;

    private Serde<String> stringSerde = Serdes.String();
    private Serde<Long> longSerde = Serdes.Long();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Before
    public void setup() throws IOException {
        final StreamsBuilder builder = new StreamsBuilder();

        // setup test driver
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "maxAggregation");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.STATE_DIR_CONFIG, org.apache.kafka.test.TestUtils.tempDirectory().getAbsolutePath());

        final String inputTopicName = "input";
        final String outputTopicName = "output";

        final KStream<String, Long> stream =
                builder.stream(inputTopicName, Consumed.with(stringSerde, longSerde));
        stream.groupByKey().aggregate(() -> 0.0,
                        (key, order, total) -> total + order,
                        Materialized.with(stringSerde, Serdes.Double()))
                .toStream().to(outputTopicName, Produced.with(stringSerde, Serdes.Double()));
        testDriver = new TopologyTestDriver(builder.build(), props);

        // setup test topics
        inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), longSerde.serializer());
        outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), Serdes.Double().deserializer());

        // pre-populate store
        prePopulateStore();
    }

    public void prePopulateStore() {

    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void ShouldAggregateMessages () {
        final List<Long> messages= new ArrayList<>();
        messages.add(1L);
        messages.add(2L);
        messages.add(5L);

        final List<Double> expectedValues = List.of(1.0, 3.0, 8.0);
        messages.forEach(message -> inputTopic.pipeInput("one", message));
        List<Double> actualValues = outputTopic.readValuesToList();
        Assert.assertEquals(expectedValues, actualValues);
    }

}

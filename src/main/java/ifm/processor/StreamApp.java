package ifm.processor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Properties;

public class StreamApp {

    public static void main (String [] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "ifmprocessor");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textlines = builder.stream("input-topic-ifm");


        KStream<Windowed<String>, String> test = textlines.selectKey((key, value) -> "ifm")
                .groupByKey()
                .windowedBy(TimeWindows.of(5000).advanceBy(5000))
                .reduce((aggValue, newValue) -> aggValue + " " + newValue,  Materialized.<String, String, WindowStore<Bytes, byte[]>>as("trade-aggregates")).toStream();

        test.mapValues(value -> value);
        test.to("output-topic-ifm", Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class)));
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}

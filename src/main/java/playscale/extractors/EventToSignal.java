package playscale.extractors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.Logger;
import playscale.utilities.AvroUtility;

import java.io.IOException;
import java.util.Properties;

/**
 * Before running this, create the input topic and the output topic (e.g. via
 * bin/kafka-topics.sh --create ...), and write some events to the input topic (e.g. via
 * bin/kafka-console-producer.sh).
 */
public class EventToSignal {

    private final Logger logger = Logger.getLogger(EventToSignal.class);
    private Properties properties;

    public EventToSignal(Properties properties) {
        this.properties = properties;
    }

    public void run() throws IOException {

        String servers = properties.getProperty("stream.bootstrap.servers");
        String consumer = properties.getProperty("stream.topic.consumer");
        String producer = properties.getProperty("stream.topic.producer");


        // Set Stream Properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-processing"); // Required
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,servers); // Required
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName()); // Topic Value Deserializer Type
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        AvroUtility eventAvro = new AvroUtility("event_schema.avsc");
        AvroUtility countrySignalAvro = new AvroUtility("signal_schema.avsc");

        CountryStore countryStore = new CountryStore("signal_schema.avsc");

        // Instantiate a stream builder
        KStreamBuilder kStreamBuilder = new KStreamBuilder();

        // Read from streams-event-input
        KStream<Long, byte[]> event = kStreamBuilder.stream(consumer);

        // Processor Topology
        // filter for new countries and create signals (this includes byte encoding/decoding)
        KStream<Long,byte[]> eventToSignals = event.map((key, value) -> new KeyValue<>(key,eventAvro.decode(value)))
                .filter((key,value) -> countryStore.test(key,value))
                .map((key,value) -> new KeyValue<>(key,countryStore.createNewCountrySignal(value)))
                .map((key,value) -> new KeyValue<>(key,countrySignalAvro.encode(value)));

        // Write signals to output stream
        eventToSignals.to(producer);

        KafkaStreams streams = new KafkaStreams(kStreamBuilder, props);
        streams.start();

    }

}
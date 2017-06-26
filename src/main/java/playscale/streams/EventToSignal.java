package playscale.streams;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.log4j.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
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

        AvroUtility avro = new AvroUtility("event_schema.avsc");
        EventProcessor eventProcessor = new EventProcessor();

        // Instantiate a stream builder
        KStreamBuilder builder = new KStreamBuilder();

        // Read from event topic and convert byte array to GenericRecord
        KStream<Long, byte[]> eventBytes = builder.stream(consumer);
        KStream<Long,GenericRecord> event = eventBytes.mapValues(avro::decode);

        // Filter to make sure event has a user id, process it, and then filter again
        // to make sure a set of signals before publishing to signal topic
        KStream<Long,byte[]> signals = event.filter(eventProcessor)
                .map(eventProcessor::extractSignals)
                .filter(new Predicate<Long, JSONObject>() {
                    @Override
                    public boolean test(Long tid, JSONObject obj) {
                        JSONArray array = (JSONArray) obj.get("signals");
                        return (array.size() > 0);

                    }
                })
                .map((key,value) -> new KeyValue<>(key, value.toString().getBytes()));

        // write signals to output stream
        signals.to(producer);

        // create and run the stream topology
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // stop the streams application when SIGTERM signal received
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
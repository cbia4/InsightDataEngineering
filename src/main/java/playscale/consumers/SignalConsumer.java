package playscale.consumers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import playscale.utilities.AvroUtility;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;


public class SignalConsumer {

    private final Logger logger = Logger.getLogger(SignalConsumer.class);
    private Consumer<Long,byte[]> consumer;
    private Properties properties;

    public SignalConsumer(Properties properties) throws IOException {

        String subscriberList = properties.getProperty("consumer.topics");
        String servers = properties.getProperty("consumer.bootstrap.servers");
        String timeout = properties.getProperty("consumer.session.timeout.ms");
        this.properties = properties;

        Properties props = new Properties();
        props.put("bootstrap.servers",servers);
        props.put("group.id", "scoring");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms",timeout);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Arrays.asList(subscriberList));


    }

    public void run() {

        long pollRate = Long.parseLong(properties.getProperty("consumer.pollrate","100"));

        while (true) {
            ConsumerRecords<Long, byte[]> records = consumer.poll(pollRate);
            for (ConsumerRecord<Long, byte[]> record : records) {
                String jsonString = new String(record.value());
                logger.info("Signal set received: " + jsonString);
            }
        }
    }

}

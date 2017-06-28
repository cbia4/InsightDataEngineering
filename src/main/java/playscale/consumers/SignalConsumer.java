package playscale.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * This is a runnable signal consumer class
 * Instances of this class are created by the SignalConsumerGroup
 * The consumer will run indefinitely until it receives a WakeupException
 * when the SignalConsumerGroup calls shutdown() for each instance of the consumer
 */
public class SignalConsumer implements Runnable {

    private final Logger logger = Logger.getLogger(SignalConsumer.class);
    private final KafkaConsumer<Long,byte[]> consumer;
    private final List<String> topics;

    public SignalConsumer(Properties configProperties) {

        this.topics = Arrays.asList(configProperties.getProperty("consumer.topic"));
        String servers = configProperties.getProperty("consumer.bootstrap.servers","localhost:9092");
        String groupId = configProperties.getProperty("consumer.group.id");

        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id",groupId);
        props.put("key.deserializer", LongDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());

        // commit offset every second to reduce duplicate records being processed (in the event of a consumer crash)
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        // this sets the amount of time the consumer has to respond to the kafka coordinator
        // before the consumer is considered "dead" and kafka re-balances the group
        props.put("session.timeout.ms","60000");
        this.consumer = new KafkaConsumer<>(props);
    }

    // get records from the specified topic until a WakeupException is thrown from the SignalConsumerGroup
    @Override
    public void run() {
        try {
            consumer.subscribe(topics);

            while(true) {
                ConsumerRecords<Long,byte[]> records = consumer.poll(Long.MAX_VALUE);
                for(ConsumerRecord<Long,byte[]> record : records) {
                    String jsonString = new String(record.value());
                    logger.info("Signal received: " + jsonString);
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }
}

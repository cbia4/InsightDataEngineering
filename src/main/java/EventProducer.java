/**
 * Created by colinbiafore on 6/7/17.
 */

import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


class EventProducer {

    private Producer<String, String> producer;

    EventProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("acks","all");
        props.put("retries",0);
        props.put("batch.size",16384);
        props.put("linger.ms",1);
        props.put("buffer.memory",33554432);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    void run() {
        for(int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>("event", Integer.toString(i), Integer.toString(i)));
        }

        producer.close();
    }


}

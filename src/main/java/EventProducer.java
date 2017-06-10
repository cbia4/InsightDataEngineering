/**
 * Created by colinbiafore on 6/7/17.
 */

import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class EventProducer {

    // Singleton
    private EventProducer() {}

    public static void produce(List<String> eventList) {
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("acks","all");
        props.put("retries",0);
        props.put("batch.size",16384);
        props.put("linger.ms",1);
        props.put("buffer.memory",33554432);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        Producer<String,String> producer = new KafkaProducer<>(props);


        for(String event : eventList) {
            producer.send(new ProducerRecord<String, String>("event",event,event));
        }

        producer.close();
    }





}

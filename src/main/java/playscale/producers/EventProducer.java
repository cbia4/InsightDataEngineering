package playscale.producers;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.log4j.Logger;
import playscale.utilities.AvroUtility;
import playscale.utilities.S3Utility;


public class EventProducer {

    private final Logger logger = Logger.getLogger(EventProducer.class);
    private Producer<Long,byte[]> producer;
    private Properties properties;

    public EventProducer(Properties properties) {

        // Unmarshall config.properties
        String servers = properties.getProperty("producer.bootstrap.servers");
        String acks = properties.getProperty("producer.acks");
        String compression = properties.getProperty("producer.compression.type","none");
        int batchSize = Integer.parseInt(properties.getProperty("producer.batch.size","16384"));
        long linger = Long.parseLong(properties.getProperty("producer.linger.ms","1"));
        long buffMem = Long.parseLong(properties.getProperty("producer.buffer.memory","33554432"));

        // Set producer properties
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers",servers);
        producerProps.put("acks",acks);
        producerProps.put("compression.type",compression);
        producerProps.put("retries",0);
        producerProps.put("batch.size",batchSize);
        producerProps.put("linger.ms",linger);
        producerProps.put("buffer.memory",buffMem);
        producerProps.put("key.serializer","org.apache.kafka.common.serialization.LongSerializer");
        producerProps.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");

        this.producer = new KafkaProducer<>(producerProps);
        this.properties = properties;

    }

    public void run() throws IOException {

        // topic to produce to
        String topic = properties.getProperty("producer.topic");

        // Unmarshall AWS properties
        String key = properties.getProperty("aws.client.key");
        String secret = properties.getProperty("aws.client.secret");
        String region = properties.getProperty("aws.client.region");
        String bucket = properties.getProperty("aws.s3.bucket");
        String prefix = properties.getProperty("aws.s3.prefix");

        S3Utility s3 = new S3Utility(key,secret,region);

        // Get list of keys in window we want to replay
        List<String> keys = s3.getKeyList(bucket,prefix);

        // Create an Avro Utility
        AvroUtility avro = new AvroUtility("event_schema.avsc");


        // Create a list to hold generic records & a tmp variable for record streams
        Iterator<GenericRecord> recordIterator;
        InputStream recordStream;


        // Time
        long ingestStart = System.nanoTime();
        long totalRecords = 0;

        // Move events from S3 -> Kafka
        for (String objectKey : keys) {

            logger.info("Downloading: " + objectKey);
            long iterStart = System.nanoTime();

            // Download file
            recordStream = s3
                    .getObject(bucket,objectKey)
                    .getObjectContent();

            logger.info("Done. Download took " + ( (System.nanoTime() - iterStart) / 1000000 ) + " ms.");
            logger.info("Deserializing: " + objectKey);
            iterStart = System.nanoTime();

            // Deserialize file and return iterator
            recordIterator = avro.getRecords(recordStream);

            logger.info("Done. Deserialization took " + ( (System.nanoTime() - iterStart) / 1000000 ) + " ms.");
            logger.info("Sending Records.");
            iterStart = System.nanoTime();

            long ctr = 0;
            // Send event records to Kafka brokers
            while (recordIterator.hasNext()) {
                ctr++;
                GenericRecord record = recordIterator.next();
                long tenantId = (long) record.get("tenant_id");
                producer.send(new ProducerRecord<>(topic,tenantId,avro.encode(record)));
            }

            logger.info("Done. Sending Records took " + ( (System.nanoTime() - iterStart) / 1000000 ) + " ms.");
            logger.info("Sent " + ctr + " Records.");
            totalRecords += ctr;
        }

        long ingestEnd = System.nanoTime();
        long ingestTime = (ingestEnd - ingestStart) / 1000000000;
        logger.info("Job Complete.");
        logger.info("------Statistics------");
        logger.info("Time Elapsed: " + ingestTime + " seconds");
        logger.info("Files Read: " + keys.size() + " files");
        logger.info("Records Sent: " + totalRecords + " records");
        logger.info("Throughput: " + (totalRecords / ingestTime) + " records/second");
        logger.info("----------------------");

        // clean up client connections
        s3.shutdown();
        producer.close();
    }

}

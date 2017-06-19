import org.apache.avro.generic.GenericRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;


/**
 * Created by colinbiafore on 6/8/17.
 */
public class ReplayPipeline {
    public static void main(String[] args) throws IOException {

        Properties prop = new Properties();
        InputStream is = new FileInputStream("config.properties");
        prop.load(is);
        is.close();

        // Read from config.properties
        String key = prop.getProperty("aws.client.key");
        String secret = prop.getProperty("aws.client.secret");
        String region = prop.getProperty("aws.client.region");

        String bucket = prop.getProperty("aws.s3.bucket");
        String prefix = prop.getProperty("aws.s3.prefix");

        S3Client s3Client = new S3Client(key,secret,region);

        // Get list of keys in window we want to replay
        List<String> keys = s3Client.getKeyList(bucket,prefix);

        // Create an Avro Utility
        AvroUtility avro = new AvroUtility("src/main/resources/event_schema.avsc");

        // Create Kafka Event Producer
        EventProducer eventProducer = new EventProducer();

        // Create a list to hold generic records & a tmp variable for record streams
        Iterator<GenericRecord> recordIterator;
        InputStream recordStream;


        // Time
        long ingestStart = System.nanoTime();

        // Move events from S3 -> Kafka
        for (String objectKey : keys) {
            long iterStart = System.nanoTime();
            recordStream = s3Client
                    .getObject(bucket,objectKey)
                    .getObjectContent(); // Get a stream of object content from S3

            recordIterator = avro.getRecords(recordStream); // get an iterator for the list of records

            while (recordIterator.hasNext()) {
                eventProducer.send(avro.encode(recordIterator.next())); // send records one by one to Kafka for processing
            }
            long iterEnd = System.nanoTime();
            long iterTime = (iterEnd - iterStart) / 1000000000;
            System.out.println("Time Elapsed for File: " + iterTime + " seconds");
        }

        long ingestEnd = System.nanoTime();

        long ingestTime = (ingestEnd - ingestStart) / 1000000000;

        System.out.println("Time Elapsed: " + ingestTime + " seconds");

        // clean up client connections
        eventProducer.close();
        s3Client.shutdownS3Client();

    }
}

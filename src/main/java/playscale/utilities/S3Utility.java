package playscale.utilities;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * A utility class to interact with Amazon S3
 * Security Credentials are configured in the config.properties file
 * and passed to the S3Utility constructor
 */
public class S3Utility {


    private final Logger logger = Logger.getLogger(S3Utility.class);
    private AmazonS3 client;

    public S3Utility(String key, String secret, String region) {
        AWSCredentials credentials = new BasicAWSCredentials(key,secret);
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setMaxConnections(1000000);
        this.client = AmazonS3Client.builder()
                .withRegion(region)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withClientConfiguration(configuration)
                .build();
    }


    // use the s3 client to download an object
    public S3Object getObject(String bucket, String key) { return client.getObject(bucket,key); }

    public void shutdown() { client.shutdown(); }

    // get a set of keys in an S3 bucket at a specified prefix
    public List<String> getKeyList(String bucket, String prefix) {

        List<S3ObjectSummary> summaries = null;
        List<String> keys = new ArrayList<String>();

        try {
            ObjectListing listing = client.listObjects(bucket,prefix);
            summaries = listing.getObjectSummaries();

            while(listing.isTruncated()) {
                listing = client.listNextBatchOfObjects(listing);
                summaries.addAll(listing.getObjectSummaries());
            }
        } catch (AmazonServiceException ase) {
            logger.error("Amazon Service Exception. Exiting.");
            ase.printStackTrace();
            System.exit(-1);
        } catch (AmazonClientException ace) {
            logger.error("Amazon Client Exception. Exiting.");
            ace.printStackTrace();
            System.exit(-1);
        }

        // filter out any objects that are not in avro format
        String avroFileExtension = "part.avro";
        for(S3ObjectSummary objectSummary : summaries) {
            if(objectSummary.getKey().endsWith(avroFileExtension))
                keys.add(objectSummary.getKey());
        }

        return keys;
    }
}

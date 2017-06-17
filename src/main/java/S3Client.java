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

import java.util.ArrayList;
import java.util.List;

/**
 * Created by colinbiafore on 6/16/17.
 */
public class S3Client {


    private AmazonS3 client;
    private String key;
    private String secret;
    private String region;

    public S3Client(String key, String secret, String region) {
        this.key = key;
        this.secret = secret;
        this.region = region;
        AWSCredentials credentials = new BasicAWSCredentials(this.key,this.secret);
        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setMaxConnections(500);
        this.client = AmazonS3Client.builder()
                .withRegion(this.region)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withClientConfiguration(configuration)
                .build();
    }


    public S3Object getObject(String bucket, String key) { return client.getObject(bucket,key); }

    public void shutdownS3Client() { client.shutdown(); }

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
            System.out.println("AmazonServiceException thrown!!");
            ase.printStackTrace();
            System.exit(-1);
        } catch (AmazonClientException ace) {
            System.out.println("AmazonClientException thrown!!");
            ace.printStackTrace();
            System.exit(-1);
        }

        String avroFileExtension = "part.avro";
        for(S3ObjectSummary objectSummary : summaries) {
            if(objectSummary.getKey().endsWith(avroFileExtension))
                keys.add(objectSummary.getKey());
        }

        return keys;
    }





}

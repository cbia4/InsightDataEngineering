import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Created by colinbiafore on 6/8/17.
 */
public class S3Connector {

    // Singleton
    private S3Connector(){}

    public static void run() {

        // returns default credentials
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();

        // TODO: Replace AmazonS3Client with non-deprecated equivalent
        AmazonS3 s3client = new AmazonS3Client(credentials);

        // TODO: Read String literals from configuration
        String bucketName = "castle-kafka-export";
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(2);
        ListObjectsV2Result result;
        do {
            result = s3client.listObjectsV2(req);

            for(S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + " " + "(size = " + objectSummary.getSize() + ")");
            }
            System.out.println("Next Continuation Token : " + result.getNextContinuationToken());
            req.setContinuationToken(result.getNextContinuationToken());
        } while(result.isTruncated());
    }
}

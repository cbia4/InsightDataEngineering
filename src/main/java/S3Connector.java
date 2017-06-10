import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;

/**
 * Created by colinbiafore on 6/8/17.
 */
public class S3Connector {

    private static String bucket;
    private static String key;

    // Singleton
    private S3Connector(){}

    private static void readConfig() throws IOException {
        InputStream fis = new FileInputStream("src/main/resources/config");
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));
        bucket = br.readLine();
        key = br.readLine();
    }

    public static InputStream getObjectContent() {

        try {
            readConfig();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        // returns default credentials
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
        AmazonS3 s3client = AmazonS3Client.builder()
                .withRegion("us-east-1")
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();


        S3Object object = s3client.getObject(bucket,key);

        return object.getObjectContent();

    }
}

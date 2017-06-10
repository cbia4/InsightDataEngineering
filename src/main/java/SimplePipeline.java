import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Created by colinbiafore on 6/8/17.
 */
public class SimplePipeline {
    public static void main(String[] args) throws IOException {
        InputStream s3content = S3Connector.getObjectContent();
        List<String> eventList = AvroReader.deserialize(s3content);
        EventProducer.produce(eventList);
    }
}

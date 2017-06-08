import org.junit.Test;

/**
 * Created by colinbiafore on 6/7/17.
 */
public class EventProducerTest {

    @Test
    public void testProducer() {
        EventProducer ep = new EventProducer();
        ep.run();
    }
}

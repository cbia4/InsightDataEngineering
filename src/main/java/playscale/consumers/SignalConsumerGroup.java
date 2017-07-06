package playscale.consumers;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Consumer Group Class
 * The run() is called from the PlayScale driver when "consumer" is selected
 * This class will create the number of consumers specified in the config.properties file
 * under the specified consumer group id - consumers will consume from the topics given
 */
public class SignalConsumerGroup {

    private final Logger logger = Logger.getLogger(SignalConsumerGroup.class);
    private final int numConsumers;
    private final String groupId;
    private final Properties configProperties;

    public SignalConsumerGroup(Properties configProperties) {
        this.configProperties = configProperties;
        this.numConsumers = Integer.parseInt(configProperties.getProperty("consumer.group.threads","1"));
        this.groupId = configProperties.getProperty("consumer.group.id");
    }

    // Create a thread executor service, a list of consumers, and execute them one by one
    public void run() {

        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        List<SignalConsumer> consumers = new ArrayList<>();
        for(int i = 0; i < numConsumers; i++) {
            SignalConsumer consumer = new SignalConsumer(i,configProperties);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        // Safely shut down the consumer group when SIGTERM signal received
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                for(SignalConsumer consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    long timeout = 5000;
                    executor.awaitTermination(timeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    logger.error("InterruptedException thrown while shutting down Consumer Group with groupId " + groupId);
                    e.printStackTrace();
                }
            }
        });
    }



}

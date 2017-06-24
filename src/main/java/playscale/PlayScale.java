package playscale;

import org.apache.log4j.Logger;
import playscale.consumers.SignalConsumer;
import playscale.extractors.EventToSignal;
import playscale.producers.EventProducer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;



/**
 * Created by colinbiafore on 6/8/17.
 */
public class PlayScale {

    private static final Logger logger = Logger.getLogger(PlayScale.class);
    private static final String PRODUCER = "PRODUCER";
    private static final String STREAM = "STREAM";
    private static final String CONSUMER = "CONSUMER";

    private static void printUsage() {
        logger.info("Usage:");
        logger.info("Producer - Application will act as a Kafka Producer instance");
        logger.info("Stream - Application will act as a Kafka Stream instance");
        logger.info("Consumer - Application will act as a Kafka Producer instance");
    }

    public static void main(String[] args) throws IOException {

        // Make sure user specified the app segment they want to run
        if(args.length < 1 || args.length > 2) {
            logger.error("Incorrect Number of Arguments.");
            printUsage();
            System.exit(0);
        }

        // Instantiate global properties object
        Properties properties = new Properties();
        InputStream is = new FileInputStream("config.properties");
        properties.load(is);
        is.close();


        // Run the associated application segment (Producer, Stream, Consumer)
        String appType = args[0];
        if(appType.toUpperCase().equals(PRODUCER)) {
            logger.info("Starting Producer instance.");
            EventProducer eventProducer = new EventProducer(properties);
            eventProducer.run();
        }

        else if(appType.toUpperCase().equals(STREAM)) {
            logger.info("Starting Stream instance.");
            EventToSignal ets = new EventToSignal(properties);
            ets.run();
        }

        else if(appType.toUpperCase().equals(CONSUMER)) {
            logger.info("Starting Consumer instance.");
            SignalConsumer signalConsumer = new SignalConsumer(properties);
            signalConsumer.run();
        }

        else {
            logger.error("No option: " + appType);
            printUsage();
            System.exit(0);
        }

    }
}

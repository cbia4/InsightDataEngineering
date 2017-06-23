package playscale;

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

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("Producer - Application will act as a Kafka Producer instance");
        System.out.println("Stream - Application will act as a Kafka Stream instance");
        System.out.println("Consumer - Application will act as a Kafka Producer instance");
    }

    public static void main(String[] args) throws IOException {

        // Make sure user specified the app segment they want to run
        if(args.length < 1 || args.length > 2) {
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
        if(appType.toUpperCase().equals("PRODUCER")) {
            System.out.println("Starting Producer instance.");
            EventProducer eventProducer = new EventProducer(properties);
            eventProducer.run();
        }

        else if(appType.toUpperCase().equals("STREAM")) {
            System.out.println("Starting Stream instance.");
            EventToSignal ets = new EventToSignal(properties);
            ets.run();
        }

        else if(appType.toUpperCase().equals("CONSUMER")) {
            System.out.println("Starting Consumer instance.");
            SignalConsumer signalConsumer = new SignalConsumer(properties);
            signalConsumer.run();
        }

        else {
            printUsage();
            System.exit(0);
        }

    }
}

package org.sama.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class.getName());
    private static final String url = "https://stream.wikimedia.org/v2/stream/recentchange";

    public static void main(String[] args) {
        logger.info("Starting the main program");

        Properties properties = Utils.getProducerProperties();
        try {
            Producer producer = new WikimediaChangesProducer(properties);
            EventHandler eventHandler = new WikiMediaChangesEventHandler(producer); //

            EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
            EventSource eSource = builder.build();

            eSource.start();
            TimeUnit.MINUTES.sleep(5);

        } catch (InterruptedException interruptedException){
            logger.info("Interrupted Exception: ", interruptedException);
        } catch (Exception exception){
            logger.error("Unexpected exception: ", exception);
        }

    }
}

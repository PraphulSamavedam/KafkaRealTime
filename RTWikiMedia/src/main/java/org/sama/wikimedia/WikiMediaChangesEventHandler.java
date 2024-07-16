package org.sama.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiMediaChangesEventHandler implements EventHandler {

    private final Producer producer;
    final Logger log = LoggerFactory.getLogger(WikiMediaChangesEventHandler.class.getName());

    WikiMediaChangesEventHandler(Producer producer){
        this.producer = producer;
    }

    @Override
    public void onOpen() throws Exception {
        // nothing to do here
    }

    @Override
    public void onClosed() throws Exception {
        this.producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent){
        log.info("Events Handler: event: {%s} message: {%s}".formatted(event.toLowerCase(), messageEvent.getData()));
        this.producer.send(messageEvent.getData());
    }

    @Override
    public void onComment(String comment){
        // nothing to do here
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in collecting stream", t);
    }
}

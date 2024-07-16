package org.sama.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class WikimediaChangesProducer implements Producer{

    final KafkaProducer<String, String> producer;
    final String topic = "wikimedia_producer";

    final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class.getName());


    WikimediaChangesProducer(Properties properties) throws InterruptedException {
        this.producer = new KafkaProducer<>(properties);
    }

    @Override
    public void close() {
        this.producer.close();
    }

    @Override
    public void send(String data) {
        this.log.info("On topic: {%s} sending data:\n{%s}".formatted(topic, data));
        this.producer.send(new ProducerRecord<>(topic, data));
    }


}

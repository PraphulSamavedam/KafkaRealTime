package org.sama.kafkaBasics;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
* This class represents a Java producer which produces to the topic 'demo-topic'
* */
public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Hello World");

        // Create producer properties localhost = 127.0.0.1
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo-topic", "Hello-World-New");

        // Send data -- asynchronous
        producer.send(producerRecord);

        // Flush and close producer
        // Synchronous operation to send all data and block until data Rarely used
        producer.flush();

        // flush and close the data.
        producer.close();
    }
}

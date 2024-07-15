package org.sama.kafkaBasics;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/*
* This class represents a Java producer which produces to the topic 'demo-topic'
* */
public class ProducerWithCallBack {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallBack.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Java Producer with Call back");

        // Create producer properties localhost = 127.0.0.1
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty("batch.size", "100");


        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        System.out.println("KeySet: " + properties.keySet());

        for (int j = 0; j < 5; j++) {

            for (int i = 0; i < 10; i++) {

                String topic = "demo-topic";
                String key = "id_" + i;
                String value = "Value: \n" + i;

                // Create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // Send data -- asynchronous
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception except) {
                        // execute every time a record is successfully
                        if (except == null) {
                            log.info("Received new metadata: \n" +
                                    "Key:" + key + " || Partition:" + metadata.partition() + "\n");

                        } else {
                            log.error("Error Occurred while producing: " + except);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }



        // Flush and close producer
        // Synchronous operation to send all data and block until data Rarely used
        producer.flush();

        // flush and close the data.
        producer.close();
    }
}

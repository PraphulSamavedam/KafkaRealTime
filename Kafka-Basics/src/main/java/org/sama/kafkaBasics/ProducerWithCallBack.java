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


        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        System.out.println("KeySet: " + properties.keySet());

        for (int i= 0; i< 1; i ++){
            // Create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo-topic", "Record: " + i);

            // Send data -- asynchronous
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception except) {
                    // execute every time a record is successfully
                    if (except == null){
                        log.info("Received new metadata: \n" +
                                "Topic:" + metadata.topic() + "\n" +
                                "Partition:" + metadata.partition() + "\n" +
                                "Offset:" + metadata.offset() + "\n" +
                                "ValueSize(int):" + metadata.serializedValueSize() + "\n" +
                                "Timestamp:" + metadata.timestamp() + "\n"
                        );
                    } else {
                        log.error("Error Occurred while producing: " + except);
                    }
                }
            });
        }



        // Flush and close producer
        // Synchronous operation to send all data and block until data Rarely used
        producer.flush();

        // flush and close the data.
        producer.close();
    }
}

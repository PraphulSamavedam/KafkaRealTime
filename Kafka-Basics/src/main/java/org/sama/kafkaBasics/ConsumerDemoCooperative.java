package org.sama.kafkaBasics;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/*
This class represents a never ending consumer. This is just for demo.
 */
public class ConsumerDemoCooperative{

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Consumer Demo with shutdown and Co-operative reassignment");

        final String groupId = "application-group-new";

        AbstractConsumer.setGroupId(groupId);
        Properties properties = AbstractConsumer.setUp();
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                CooperativeStickyAssignor.class.getName());

        // Create KafkaConsumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();

        // When Wakeup is caused, we need to join back to the main thread.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown request, try to shutdown consumer by calling consumer.wakeup()");

            consumer.wakeup();

            // Join the main thread to continue with the execution of the main thread
            try{
                mainThread.join();
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            }
        }));

        try {
            // Subscribe to a topic
            consumer.subscribe(List.of(AbstractConsumer.topic));

            // Consume the data
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> consumerRecord: records) {
                    log.info("Partition: %d, Offset: %d, Key: %s, Value: %s\n".formatted(
                            consumerRecord.partition(), consumerRecord.offset(),
                            consumerRecord.value(), consumerRecord.value()));
                }

            }

        } catch (WakeupException wakeupException) {
            log.info("Consumer is preparing to shut down.");
        } catch (Exception e){
            log.error("Unexpected Exception " + e.getMessage());
        }
        finally {
            consumer.close(); // Close the consumer and commits the offsets
            log.info("Consumer is now gracefully shutdown.");
        }
    }
}

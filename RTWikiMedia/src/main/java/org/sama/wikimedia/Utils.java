package org.sama.wikimedia;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * This class represents utilities which can be reused for other purposes.
 */
public class Utils{

    private static void setBootstrapServersProperty(Properties properties){
        properties.setProperty("bootstrap.servers", "localhost:9092");
    }

    /**
     * This function returns the Properties of the producer such as bootstrap servers to connect, serializers
     *
     * @return Properties of producer
     */
    static Properties getProducerProperties(){
        Properties properties = new Properties();
        setBootstrapServersProperty(properties);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*10124));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        return (Properties) properties.clone();
    }

    /**
     * This function returns the Properties of the consumer such as bootstrap servers to connect, serializers
     *
     * @return Properties of producer
     */
    static Properties getConsumerProperties(){
        Properties properties = new Properties();
        setBootstrapServersProperty(properties);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return (Properties) properties.clone();
    }
}

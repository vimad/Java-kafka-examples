package com.vinod.kaka.basic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";

        // Create Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        // Create Producer Record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>("first_topic", "hello world"); // can add a key as 2nd argument

        // Send data - asynchronous --call .get() method to make it synchronous
        producer.send(producerRecord);

        producer.flush();
        producer.close();
    }
}

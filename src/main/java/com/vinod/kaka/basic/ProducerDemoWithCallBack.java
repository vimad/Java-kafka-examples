package com.vinod.kaka.basic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

        // Create Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);


        for(int i = 0; i < 5; i++) {
            logger.info("/n **************************************************");
            // Create Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("first_topic", "hello world " + i);

            // Send data - asynchronous
            producer.send(producerRecord, (recordMetaData, exception ) -> {
                if (exception == null) {
                    logger.info("Meta data received \n" +
                            "Topic " + recordMetaData.topic() +"\n" +
                            "Partition " + recordMetaData.partition() +"\n" +
                            "Offset " + recordMetaData.offset() +"\n" +
                            "Timestamp " + recordMetaData.timestamp() +"\n");
                } else {
                    logger.error("Error receiving data");
                }
            });
        }

        producer.flush();
        producer.close();
    }
}

package com.vinod.kaka.basic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    public static void main(String[] args) {

        new ConsumerDemoWithThread().run();

    }

    public ConsumerDemoWithThread(){}

    public void run () {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        String bootstrapServers = "localhost:9092";
        String groupId = "my_second_group";
        String topic = "first_topic";

        CountDownLatch latch = new CountDownLatch(1);

        Runnable runnable = new ConsumerRunnable(latch,bootstrapServers,groupId, topic);

        Thread thread = new Thread(runnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable)runnable).shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interrupted ", e);
        } finally {
            logger.info("Application closed");
        }
    }

    public class ConsumerRunnable implements Runnable{
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

        private CountDownLatch countDownLatch;

        KafkaConsumer<String, String> consumer;

        ConsumerRunnable(CountDownLatch countDownLatch,
                         String bootstrapServers,
                         String groupId,
                         String topic) {
            this.countDownLatch = countDownLatch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            consumer = new KafkaConsumer<String, String>(properties);

            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("key: " + record.key() + " value: " + record.value() + " Partition " + record.partition());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received Shutdown signal");
            } finally {
                consumer.close();
                countDownLatch.countDown();
            }
        }

        public void shutDown() {
            // This method to interrupt consumer.poll
            // This will throw WakeupException
            consumer.wakeup();
        }
    }
}

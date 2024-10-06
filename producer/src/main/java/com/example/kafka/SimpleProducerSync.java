package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleProducerSync {

    public static final Logger log = LoggerFactory.getLogger(SimpleProducerSync.class);

    public static void main(String[] args) {
        // KafkaProducer config 설정

        String topic = "my-topic";

        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // key : null

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // ProducerRecord 객체 생성
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "Hello World");

        // KafkaProducer message send

        try {
            RecordMetadata recordMetadata = producer.send(producerRecord).get();
            log.info("result : {}", recordMetadata);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            producer.close();
        }

    }
}

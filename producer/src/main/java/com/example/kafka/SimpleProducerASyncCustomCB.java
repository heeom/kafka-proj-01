package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducerASyncCustomCB {

    public static final Logger log = LoggerFactory.getLogger(SimpleProducerASyncCustomCB.class);

    public static void main(String[] args) {
        // KafkaProducer config 설정

        String topic = "multipart-topic";

        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // key : null

        // KafkaProducer 객체 생성
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        for (int seq = 0; seq < 20; seq++) {

            // ProducerRecord 객체 생성
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topic, seq, "Hello World" + seq);
            CustomCallback customCallback = new CustomCallback(seq);

            // KafkaProducer message send
            producer.send(producerRecord, customCallback);
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.close();

    }
}

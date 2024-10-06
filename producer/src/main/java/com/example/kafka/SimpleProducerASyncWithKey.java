package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducerASyncWithKey {

    public static final Logger log = LoggerFactory.getLogger(SimpleProducerASyncWithKey.class);

    public static void main(String[] args) {
        // KafkaProducer config 설정

        String topic = "multipart-topic";

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

        for (int seq = 0; seq < 20; seq++) {
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("partition : {},\n\n offset: {}, \n\n timestamp : {}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    } else {
                        log.error("error ", e);
                    }
                }
            });
        }


        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.close();

    }
}

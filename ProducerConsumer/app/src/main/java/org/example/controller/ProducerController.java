package org.example.controller;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.*;

import java.util.Properties;

@RestController
@RequestMapping("/producer")
public class ProducerController {
    private static final String TOPIC_NAME = "testy";
    private static final String BOOTSTRAP_SERVERS = "3.109.197.152:9092";

    private final Producer<String, String> kafkaProducer;

    public ProducerController() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.kafkaProducer = new KafkaProducer<>(props);
    }

    @PostMapping("/event/{eventType}")
    public void sendEventToKafka(@PathVariable String eventType, @RequestBody String eventData) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, eventType, eventData);
        kafkaProducer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Error sending message to Kafka: " + exception.getMessage());
            } else {
                System.out.println("Message sent to Kafka, offset: " + metadata.offset());
            }
        });
    }
}
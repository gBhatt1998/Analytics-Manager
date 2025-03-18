package org.example.controller;

import io.prometheus.client.Counter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class ConsumerController {

    private final Map<String, Counter> eventCounters = new HashMap<>();

    public ConsumerController() {
        registerCounter("userEvent");
        registerCounter("userEnroll");
    }

    private void registerCounter(String eventType) {
        eventCounters.put(eventType, Counter.build()
                .name("kafka_" + eventType + "_received_total")
                .help("Total number of " + eventType + " events received")
                .register());
    }

    @KafkaListener(topics = "testy", groupId = "metrics-consumer-group")
    public void listen(ConsumerRecord<String, String> record) {
        String eventType = record.key(); // Extract event type from Kafka key
        String eventData = record.value(); // Extract event data from Kafka value

        System.out.println("Received event [" + eventType + "]: " + eventData);

        // Increment the correct Prometheus counter if the key is recognized
        if (eventType != null && eventCounters.containsKey(eventType)) {
            eventCounters.get(eventType).inc();
        } else {
            System.err.println("Unknown event type: " + eventType);
        }
    }
}
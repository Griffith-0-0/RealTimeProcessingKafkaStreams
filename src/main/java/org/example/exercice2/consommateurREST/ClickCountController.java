package org.example.exercice2.consommateurREST;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RestController
public class ClickCountController {

    private final Map<String, Long> clickCounts = new ConcurrentHashMap<>();

    @KafkaListener(topics = "click-counts", groupId = "click-api")
    public void listen(ConsumerRecord<String, Long> record) {
        clickCounts.put(record.key(), record.value());
    }

    @GetMapping("/clicks/count")
    public Map<String, Long> getClickCounts() {
        return clickCounts;
    }
}


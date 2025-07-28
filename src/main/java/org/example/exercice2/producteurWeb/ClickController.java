package org.example.exercice2.producteurWeb;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClickController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC = "click";
    @PostMapping("/click")
    public ResponseEntity<String> click(@RequestParam String userid) {
        kafkaTemplate.send(TOPIC, userid, "click");
        return ResponseEntity.ok("Click enregistré pour l'utilisateur : " + userid);
    }
}

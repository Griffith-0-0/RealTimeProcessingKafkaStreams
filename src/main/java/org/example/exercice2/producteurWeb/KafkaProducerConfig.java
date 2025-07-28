package org.example.exercice2.producteurWeb;


import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

// Annotation Spring pour indiquer que cette classe contient des beans de configuration
@Configuration
public class KafkaProducerConfig {

    // Déclare un bean qui crée une ProducerFactory (fabrique de producteurs Kafka)
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        // Création de la configuration du producteur Kafka
        Map<String, Object> config = new HashMap<>();

        // Adresse du serveur Kafka (localhost:9092 si tu utilises Docker en local)
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Sérialiseur de la clé : ici on envoie la clé en tant que chaîne (String)
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Sérialiseur de la valeur : ici aussi en tant que chaîne
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Création de la factory à partir de cette configuration
        return new DefaultKafkaProducerFactory<>(config);
    }

    // Déclare un bean KafkaTemplate : permet d'envoyer des messages facilement
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        // Utilise la factory précédente pour créer le KafkaTemplate
        return new KafkaTemplate<>(producerFactory());
    }
}


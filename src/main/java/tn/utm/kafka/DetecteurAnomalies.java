package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class DetecteurAnomalies {

    private static final String TOPIC_SOURCE = "pos-events";
    private static final String TOPIC_ALERTES = "alertes-retours";
    private static final String GROUP = "alerte-1";
    private static final double SEUIL_ALERTE = 200.0;
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {

        // Configuration Consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Configuration Producer (pour écrire les alertes)
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(Collections.singletonList(TOPIC_SOURCE));
            System.out.println("⏳ DetecteurAnomalies démarré — seuil alerte : " + SEUIL_ALERTE + " DT");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        EvenementPOS event = mapper.readValue(record.value(), EvenementPOS.class);

                        if (event.getType().equals("RETOUR") && event.getMontant() > SEUIL_ALERTE) {
                            System.out.printf("🚨 ALERTE — Retour anormal : %.2f DT | %s | %s%n",
                                event.getMontant(), event.getVille(), event.getIdCaisse());

                            // Envoyer l'alerte dans le topic alertes-retours
                            String alerte = mapper.writeValueAsString(event);
                            producer.send(new ProducerRecord<>(TOPIC_ALERTES, event.getVille(), alerte));
                        }

                    } catch (Exception e) {
                        System.err.println("Erreur parsing : " + e.getMessage());
                    }
                }

                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        }
    }
}
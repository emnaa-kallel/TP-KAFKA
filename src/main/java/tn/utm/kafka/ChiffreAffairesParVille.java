package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class ChiffreAffairesParVille {

    private static final String TOPIC = "pos-events";
    private static final String GROUP = "ca-1";
    private static final ObjectMapper mapper = new ObjectMapper();

    // Map ville → CA cumulé
    private static final Map<String, Double> caParVille = new HashMap<>();
    private static long dernierAffichage = System.currentTimeMillis();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            System.out.println("⏳ ChiffreAffaires démarré — groupe : " + GROUP);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        EvenementPOS event = mapper.readValue(record.value(), EvenementPOS.class);
                        traiterEvenement(event);
                    } catch (Exception e) {
                        System.err.println("Erreur parsing : " + e.getMessage());
                    }
                }

                // Commit manuel après chaque batch
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }

                // Afficher le CA toutes les 5 secondes
                long maintenant = System.currentTimeMillis();
                if (maintenant - dernierAffichage >= 5000) {
                    afficherCA();
                    dernierAffichage = maintenant;
                }
            }
        }
    }

    private static void traiterEvenement(EvenementPOS event) {
        String ville = event.getVille();
        caParVille.putIfAbsent(ville, 0.0);

        if (event.getType().equals("VENTE")) {
            caParVille.merge(ville, event.getMontant(), Double::sum);
        } else if (event.getType().equals("RETOUR")) {
            caParVille.merge(ville, -event.getMontant(), Double::sum);
        }
        // OUVERTURE ignoré pour le CA
    }

    private static void afficherCA() {
        System.out.println("\n========== CHIFFRE D'AFFAIRES PAR VILLE ==========");
        caParVille.entrySet().stream()
            .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
            .forEach(e -> System.out.printf("  %-12s : %10.2f DT%n", e.getKey(), e.getValue()));
        System.out.println("===================================================\n");
    }
}
package tn.utm.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class SimulateurCaisse {

    private static final String[] VILLES = {"Tunis", "Sousse", "Sfax", "Bizerte", "Gabes"};
    private static final String TOPIC = "pos-events";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Random random = new Random();

    public static void main(String[] args) throws Exception {

        String idCaisse = "CAISSE-" + (100 + random.nextInt(900));
        System.out.println("Démarrage simulateur : " + idCaisse);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                String ville = VILLES[random.nextInt(VILLES.length)];
                String type = choisirType();
                double montant = (type.equals("OUVERTURE")) ? 0 : 5 + Math.round(random.nextDouble() * 495 * 100.0) / 100.0;

                EvenementPOS event = new EvenementPOS(
                    type,
                    idCaisse,
                    ville,
                    Instant.now().toString(),
                    montant
                );

                String json = mapper.writeValueAsString(event);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, ville, json);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Erreur envoi : " + exception.getMessage());
                    } else {
                        System.out.printf("✓ [%s] %s | %.2f DT → partition %d%n",
                            ville, type, montant, metadata.partition());
                    }
                });

                // Attendre entre 100 et 500 ms
                Thread.sleep(100 + random.nextInt(400));
            }
        }
    }

    private static String choisirType() {
        int n = random.nextInt(100);
        if (n < 70) return "VENTE";
        else if (n < 80) return "RETOUR";
        else return "OUVERTURE";
    }
}
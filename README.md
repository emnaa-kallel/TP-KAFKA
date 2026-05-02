# Pipeline de Logs POS en Temps Réel — Apache Kafka

## Description

Ce projet implémente un pipeline de traitement de données en temps réel basé sur Apache Kafka. Il simule des événements de vente provenant de caisses (POS) réparties dans plusieurs villes tunisiennes, calcule le chiffre d'affaires en continu et détecte les retours anormaux.

## Architecture

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ Caisse POS 1 │   │ Caisse POS 2 │   │ Caisse POS 3 │
│ (Producer)   │   │ (Producer)   │   │ (Producer)   │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                  │                  │
       └──────────────────┴──────────────────┘
                          │
                          ▼
              ┌────────────────────┐
              │  Topic: pos-events │
              │  (4 partitions)    │
              │  clé = ville       │
              └─────────┬──────────┘
                        │
          ┌─────────────┴──────────────┐
          ▼                            ▼
┌──────────────────┐        ┌────────────────────┐
│ ChiffreAffaires  │        │ DetecteurAnomalies  │
│ groupe : ca-1    │        │ groupe : alerte-1   │
└──────────────────┘        └────────┬───────────┘
                                     │
                                     ▼
                         ┌───────────────────────┐
                         │ Topic: alertes-retours │
                         └───────────────────────┘
```

## Prérequis

| Outil | Version minimale | Lien |
|-------|-----------------|------|
| Java JDK | 17 | https://adoptium.net |
| Apache Kafka | 3.9.0 | https://kafka.apache.org/downloads |
| Apache Maven | 3.8+ | https://maven.apache.org/download.cgi |

## Structure du projet

```
pos-pipeline/
├── pom.xml
├── README.md
└── src/
    └── main/
        └── java/
            └── tn/utm/kafka/
                ├── EvenementPOS.java
                ├── SimulateurCaisse.java
                ├── ChiffreAffairesParVille.java
                └── DetecteurAnomalies.java
```

## Procédure de démarrage

### Étape 1 — Démarrer le broker Kafka

```powershell
C:\kafka\bin\windows\kafka-server-start.bat C:\kafka-data\server.properties
```

Attendez le message : `[KafkaServer id=1] started`

### Étape 2 — Créer les topics

```powershell
C:\kafka\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic pos-events --partitions 4 --replication-factor 1

C:\kafka\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --create --topic alertes-retours --partitions 2 --replication-factor 1
```

### Étape 3 — Compiler le projet

```powershell
cd C:\projets\pos-pipeline
mvn clean package
```

### Étape 4 — Lancer les 3 composants (3 terminaux séparés)

```powershell
# Terminal 2 - Simulateur
mvn exec:java "-Dexec.mainClass=tn.utm.kafka.SimulateurCaisse"

# Terminal 3 - Chiffre d'affaires
mvn exec:java "-Dexec.mainClass=tn.utm.kafka.ChiffreAffairesParVille"

# Terminal 4 - Détecteur d'anomalies
mvn exec:java "-Dexec.mainClass=tn.utm.kafka.DetecteurAnomalies"
```

## Tests simples

✔ Envoi de messages en continu (simulation de caisses)
✔ Calcul du chiffre d’affaires en temps réel
✔ Détection des retours supérieurs à 200 DT

## Tests avancés

### Rebalance avec 3 instances ChiffreAffaires

Lancez 3 fois ChiffreAffairesParVille dans 3 terminaux différents, puis observez :

```powershell
C:\kafka\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group ca-1
```

### Observer le LAG

```powershell
C:\kafka\bin\windows\kafka-consumer-groups.bat --bootstrap-server localhost:9092 --describe --group ca-1
```

## Format des messages

```json
{
  "type": "VENTE",
  "idCaisse": "CAISSE-342",
  "ville": "Tunis",
  "timestamp": "2026-05-02T10:35:12.402Z",
  "montant": 175.50
}
```

Types : VENTE (70%), RETOUR (10%), OUVERTURE (20%)

## Dépannage

| Problème | Solution |
|----------|----------|
| TimeoutException | Vérifiez que le broker tourne avec `jps` |
| Consumer ne lit rien | Vérifiez le group.id et utilisez `--from-beginning` |
| Broker ne démarre pas | Supprimez `C:\kafka-data\logs` et reformatez |

## Réalisé par

EMNA KALLEL dans le cadre du TP Apache Kafka — Pipeline de logs en temps réel.

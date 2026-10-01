# Kafka Lab

Projet d'exploration d'Apache Kafka avec Java et Spring Boot.

L'application permet d'expérimenter les principaux concepts de Kafka :

- producers et consumers ;
- topics, partitions et offsets ;
- clés de messages et partitionnement ;
- consumer groups et rebalancing ;
- réplication entre plusieurs brokers ;
- élection d'un nouveau leader après une panne ;
- différence entre `acks=1` et `acks=all` ;
- mesure du débit d'un producer.

## Environnement

- Java 25, Spring Boot 4, Gradle (wrapper inclus)
- Cluster de 3 brokers Kafka (image `apache/kafka:4.3.1`, mode KRaft, chaque nœud est broker et controller) dans Docker, défini dans `compose.yml`
- Les 3 brokers tournent sur la même machine (Mac Apple Silicon)

| Topic | Partitions | Réplication | `min.insync.replicas` | Usage |
| --- | --- | --- | --- | --- |
| `orders` | 3 | 3 | 1 (défaut) | producer / consumers, clés, consumer groups |
| `benchmark` | 3 | 3 | 2 | mesure du débit |

L'application envoie avec `acks=all`.

## Lancer le projet

### 1. Démarrer le cluster

```bash
docker compose up -d
```

Les brokers sont exposés sur `localhost:29092`, `localhost:39092` et `localhost:49092`.

Le cluster n'a pas de volume : `docker compose down` efface les topics et les messages.

### 2. Créer les topics

Les topics sont créés à la main pour que leurs réglages soient connus et reproductibles.

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create --topic orders \
  --partitions 3 --replication-factor 3 \
  --bootstrap-server kafka-1:19092

docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --create --topic benchmark \
  --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2 \
  --bootstrap-server kafka-1:19092
```

### 3. Lancer l'application

```bash
./gradlew bootRun
```

### 4. Envoyer un message

```bash
curl -X POST "http://localhost:8080/orders?key=client-42" \
  -H "Content-Type: text/plain" \
  -d "commande 123"
```

La console de l'application affiche le consumer qui a reçu le message, avec sa partition et son offset. Deux messages avec la même clé arrivent toujours dans la même partition.

Le groupe `demo_group` contient 5 consumers pour 3 partitions : seuls 3 reçoivent des messages, les 2 autres restent en réserve.

### 5. Lancer le benchmark

```bash
curl -X POST http://localhost:8080/benchmark
```

## Test de panne : `acks=1` contre `acks=all`

### Objectif

Vérifier si un message confirmé au producer peut être perdu quand le leader d'une partition tombe brutalement.

### Conditions

- Cluster de 3 brokers Kafka (Docker, même machine)
- Topic dédié de **1 partition**, facteur de réplication 3, `min.insync.replicas=2`
- Envoi de 5 000 000 messages de 100 octets avec `kafka-producer-perf-test`, débit illimité
- Idempotence désactivée (`enable.idempotence=false`)
- Producer lancé depuis un broker qui n'est pas le leader

### Déroulement

1. Identifier le leader de la partition (`kafka-topics.sh --describe`)
2. Lancer l'envoi des messages
3. Pendant l'envoi, couper brutalement le leader avec `docker kill`
4. Après l'élection du nouveau leader, comparer :
    - les messages **confirmés** par le producer
    - les messages **réellement présents** dans le topic (offset de fin)
5. Refaire la même expérience avec `acks=all`

Une perte réelle correspond à un message **confirmé au producer mais absent du topic**. Les messages en erreur ne sont pas des pertes : le producer sait qu'ils ont échoué.

### Résultats

Avec `acks=1` :

| | Messages |
| --- | --- |
| Envoyés | 5 000 000 |
| Confirmés par le producer | 4 999 260 |
| Erreurs connues du producer | 740 |
| Présents après la panne | 4 998 964 |
| **Confirmés mais perdus** | **au moins 296** |

Le chiffre de 296 est un minimum : l'idempotence étant désactivée, des renvois après erreur ont pu créer des doublons, ce qui gonfle le nombre de messages présents et masque une partie des pertes.

Avec `acks=all`, la même expérience donne **0 message confirmé perdu**.

### Interprétation

Avec `acks=1`, le leader confirme le message dès qu'il l'a écrit localement, sans attendre ses replicas. S'il tombe avant qu'ils l'aient recopié, un replica en retard devient leader et le message disparaît, alors que le producer le croit enregistré.

Avec `acks=all`, le leader attend que les replicas synchronisés aient reçu le message avant de confirmer. Une panne du leader ne fait donc perdre aucun message confirmé. Le coût : plus de latence, et des erreurs visibles quand il n'y a pas assez de replicas disponibles.

**En résumé : `acks=1` peut perdre des messages en silence, `acks=all` échange un peu de performance contre la garantie qu'un message confirmé est bien conservé.**

## Benchmark de débit

Le benchmark envoie 10 000 000 de messages depuis Spring Boot vers le topic `benchmark` (3 partitions, réplication 3, `min.insync.replicas=2`), avec `acks=all`.

Chaque message contient un payload de 100 octets. Le test attend la confirmation de tous les envois avant de calculer le débit.

Le benchmark mesure :

```text
Spring Boot Producer → Kafka → confirmation de l'envoi
```

Il ne mesure pas le traitement côté consumer.

Résultats sur 4 essais :

```text
Run 1 : 630 821 msg/s
Run 2 : 658 252 msg/s
Run 3 : 657 672 msg/s
Run 4 : 629 652 msg/s
```

Résultat global :

```text
Messages par run : 10 000 000
Échecs           : 0

Débit moyen      : ~644 000 msg/s
Débit minimum    : ~630 000 msg/s
Débit maximum    : ~658 000 msg/s

Durée observée   : ~15–16 secondes par run
```

Ces résultats correspondent à un benchmark local : les 3 brokers et l'application tournent sur la même machine, sans vrai réseau entre eux. Ils ne doivent pas être interprétés comme une mesure de performance générale de Kafka.

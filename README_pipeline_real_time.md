# Pipeline Real Time — Air Traffic Streaming

Pipeline de données temps réel qui récupère des données d’avions autour de Paris, les publie dans Kafka, les transforme avec Spark Structured Streaming, puis les stocke dans PostgreSQL sous forme de tables `silver` et `gold`.


## Architecture

```text
Airplanes.live API
        │
        ▼
Python Producer
        │  topic: airplane_topic
        ▼
Apache Kafka
        │
        ▼
Spark Structured Streaming
        │
        ├── airplane_silver
        ├── airplane_gold_total
        └── airplane_gold_kpi_speed
        ▼
PostgreSQL
```

## Stack technique

- Python 3.11
- Kafka `apache/kafka:latest`
- Spark `apache/spark:3.5.6`
- PostgreSQL 16
- Docker Compose
- Librairies Python : `kafka-python`, `requests`, `pyspark`

## Structure du projet

```text
pipeline_real_time/
├── compose.yaml
├── airplane_silver.sql
├── airplane_gold_total
├── airplane_gold_kpi_speed.sql
├── guide lancement divers.txt
└── kafka_package/
    ├── DockerFile
    ├── requirement.txt
    ├── kafka_producer.py
    ├── kafka_producer_test.py
    ├── spark_consumer.py
    ├── writer.py
    └── api_package/
        ├── __init__.py
        ├── airplane_live_api.py
        └── nasa_request.py
```

## Fonctionnement

1. `airplane_live_api.py` appelle l’endpoint Airplanes.live autour des coordonnées de Paris : `48.8566, 2.3522`, rayon `250`.
2. `kafka_producer.py` publie les avions récupérés dans le topic Kafka `airplane_topic` toutes les 5 secondes.
3. `spark_consumer.py` lit le topic Kafka en streaming.
4. Spark parse le JSON, applique un schéma, ajoute des colonnes de conversion et catégorise la vitesse :
   - `slow` : vitesse sol inférieure à 200 km/h
   - `medium` : de 200 à moins de 500 km/h
   - `fast` : de 500 à moins de 700 km/h
   - `really-fast` : 700 km/h et plus
   - `unknown` : valeur absente ou non classable
5. `writer.py` écrit les données dans PostgreSQL :
   - table détaillée `airplane_silver`
   - agrégat global `airplane_gold_total`
   - agrégat par catégorie de vitesse `airplane_gold_kpi_speed`

## Tables PostgreSQL

### `airplane_silver`

Table détaillée contenant les données d’avions enrichies :

- informations avion : `hex`, `type`, `flight`, `r`, `t`, `desc`, `category`
- position : `lat`, `lon`, `alt_geom`, `alt_baro`
- vitesse et trajectoire : `gs`, `track`, `mach`, `geom_rate`
- état : `squawk`, `emergency`, `alert`, `messages`
- ingestion : `timestamp_ingest`
- transformations : `gs_km_h`, `alt_baro_km`, `alt_geom_km`, `kpi_speed_type`

### `airplane_gold_total`

Agrégat par fenêtre de 10 secondes :

- nombre approximatif d’avions distincts
- vitesse sol moyenne en km/h
- altitude géométrique moyenne en km
- nombre d’alertes
- début et fin de fenêtre

### `airplane_gold_kpi_speed`

Agrégat par fenêtre de 10 secondes et par catégorie de vitesse :

- catégorie de vitesse
- nombre approximatif d’avions distincts par catégorie
- vitesse moyenne par catégorie
- altitude moyenne par catégorie
- nombre d’alertes par catégorie

## Prérequis

- Docker
- Docker Compose
- Git

## Installation

Clonez le dépôt :

```bash
git clone https://github.com/erwanG14/pipeline_real_time.git
cd pipeline_real_time
```

Lancez l’ensemble des services :

```bash
docker compose up --build
```

Cette commande démarre :

- Kafka
- l’initialisation du topic `airplane_topic`
- le producer Python
- le consumer Spark
- PostgreSQL

## Vérifier que le pipeline fonctionne

Connectez-vous à PostgreSQL :

```bash
docker exec -it postgres psql -U spark -d airplane_db
```

Puis exécutez quelques requêtes :

```sql
SELECT * FROM airplane_silver LIMIT 10;
SELECT * FROM airplane_gold_total LIMIT 10;
SELECT * FROM airplane_gold_kpi_speed LIMIT 10;
SELECT count(*) FROM airplane_gold_kpi_speed;
```

## Tester Kafka manuellement

Lister ou créer un topic depuis le conteneur Kafka :

```bash
docker exec -it kafka_image /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Lire les messages du topic :

```bash
docker exec -it kafka_image /opt/kafka/bin/kafka-console-consumer.sh \
  --topic airplane_topic \
  --from-beginning \
  --bootstrap-server localhost:9092
```

## Lancer uniquement un producer de test

Le fichier `kafka_producer_test.py` envoie une seule série de données dans Kafka.

```bash
python -m kafka_package.kafka_producer_test
```

Attention : cette commande doit être lancée dans un environnement capable de joindre Kafka via `kafka:9092`. Depuis l’hôte local, il peut être nécessaire d’adapter `bootstrap_servers` vers `localhost:9092`.

## Configuration Docker

Le fichier `compose.yaml` définit les services suivants :

| Service | Rôle |
| --- | --- |
| `kafka` | Broker Kafka |
| `kafka-init` | Création automatique du topic `airplane_topic` |
| `producer_airplane` | Producer Python qui interroge l’API et publie dans Kafka |
| `spark-consumer` | Consumer Spark Structured Streaming qui transforme et écrit en base |
| `postgres` | Base PostgreSQL `airplane_db` |

Identifiants PostgreSQL par défaut :

```text
Host: localhost
Port: 5432
Database: airplane_db
User: spark
Password: spark
```

# Real-Time Crypto Intelligence Pipeline

## 1️⃣ Objectif du projet

Ce projet vise à concevoir et implémenter une **pipeline Data Engineering temps réel** pour l’entreprise **INVISTIS**, combinant :

- **Streaming** : Kafka + Spark Structured Streaming  
- **Batch** : Airflow + FRED API  
- **Multi-sink** : Data Lake, Supabase, Kafka (`alerts_topic`), Redis (optionnel)  
- Gestion de schémas et validation end-to-end  
- Prêt à la production pour intégration dans l’application backend de gestion de portefeuilles crypto  

---

## 2️⃣ Contexte

INVISTIS souhaite intégrer des données crypto temps réel pour permettre aux Data Scientists et aux développeurs backend de :

- Analyser les fluctuations des prix et sentiments sur le marché crypto  
- Détecter les événements de volatilité et les anomalies  
- Enrichir leurs modèles d’investissement et scoring de portefeuille  

Ce projet fait partie de la mission **DATA NEXT**, pour tester la faisabilité et la readiness production de la plateforme.  

---

## 3️⃣ Architecture

### Sources de données

- **Binance WebSocket** → Trades crypto en temps réel  
- **NewsAPI** → Articles financiers et crypto  
- **FRED API** → Indicateurs économiques pour enrichissement  

### Outputs / Destinations

- **Databricks Data Lake** → RAW + Enriched (format Parquet)  
- **Supabase** → Warehouse layer pour consommation backend  
- **Kafka `alerts_topic`** → Alertes sur événements importants  
- **Redis (optionnel)** → Cache backend pour accès rapide  

### Diagramme d’architecture (ASCII)

```text
         +-------------------+        +-----------------+
         | Binance WebSocket |        |     NewsAPI     |
         +--------+----------+        +--------+--------+
                  |                            |
                  v                            v
          +-------+----------------------------+------+
          |    Spark Structured Streaming (Databricks) |
          |  - Stream-to-Stream Join ±5 min            |
          |  - Watermark / Fenêtres 1min / 5min       |
          +------------+------------+-----------------+
                       |            |
         +-------------+            +-------------+
         |                                          |
+--------v--------+                       +---------v--------+
| Delta Lake RAW  |                       | Supabase Table   |
| & Enriched      |                       | market_enriched |
+-----------------+                       +-----------------+
         |
         v
   +-----+------+
   | Kafka Topic|
   | alerts_topic|
   +-----+------+
         |
         v
      [Redis Cache] (optionnel)

Étape 4 — Databricks (Streaming + Join)

Configuration Spark Structured Streaming avec Kafka :

CONFLUENT_BOOTSTRAP = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
CONFLUENT_API_KEY   = "JWVBQ7RG25AVCHHY"
CONFLUENT_SECRET    = "cflt9aP2o4QFY2CAugnhB/J/MMhEIDYtyGdj3Gu6dcFih68+sqYqyvBvzj8ABd4g"

Lecture des topics Kafka → trades_topic, news_topic

Stream-to-Stream Join ±5 min avec watermark

Multi-sink : Delta Lake, Supabase, Kafka alerts_topic

🔗 Lien Databricks Notebook

🔹 Étape 5 — Supabase (Warehouse Layer)

Installation package Python :

%pip install supabase

Configuration :

SUPABASE_URL = "https://TON_PROJECT_ID.supabase.co"
SUPABASE_KEY = "TON_ANON_PUBLIC_KEY"
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

Création de la table : market_enriched

Écriture des batch et stream Spark vers Supabase via foreachBatch

🔹 Étape 6 — Data Lake

RAW : /home/hajar_mamdouh/data_lake/raw/fred/

Enriched : /Volumes/invistis/datalake/raw/enriched

🔹 Étape 7 — Validation & Schema

Schéma Kafka pour trades et news défini avec StructType

Vérification des valeurs nulles, types numériques et timestamps

Test de la jointure stream-to-stream et batch → OK

6️⃣ Résultats

Kafka topics créés et fonctionnels

Stream-to-stream join Databricks exécuté

Multi-sink vers Delta Lake et Supabase confirmé

Airflow DAG pour FRED fonctionnel

Schémas validés et data flow opérationnel

7️⃣ Liens utiles

Confluent Cloud :  
https://confluent.cloud/environments/env-dz5wx1/clusters/lkc-y76m1j/overview?granularity=PT1M&interval=3600000&label=Last%20hour&refresh=60000

Databricks Notebook :  
https://dbc-679742dc-7a67.cloud.databricks.com/editor/notebooks/426871642664109?o=7474646532280705

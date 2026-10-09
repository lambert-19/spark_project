# Cahier des charges — Analyse de données Mastodon avec Apache Spark

| | |
|---|---|
| **Projet** | Pipeline Big Data temps réel + batch sur les toots Mastodon |
| **Version** | 1.0 |
| **Statut** | Brouillon à valider par l'équipe |

---

## 1. Contexte et objectifs

Construire une chaîne de traitement de bout en bout qui collecte des toots Mastodon en temps réel, les traite avec Spark (streaming **et** batch), applique un modèle de classification de sentiment (Spark MLlib) et présente les résultats sous forme de visualisations dans Jupyter.

**Objectifs mesurables**

| # | Objectif | Critère d'acceptation |
|---|---|---|
| O1 | Ingestion temps réel | Les toots arrivent dans Kafka en < 5 s après publication |
| O2 | Stream processing | Agrégats par fenêtre visibles dans PostgreSQL pendant la démo |
| O3 | Batch processing | Analyses historiques sur ≥ 10 000 toots, avec mesure d'optimisation |
| O4 | Machine Learning | Modèle de sentiment avec accuracy ≥ 75 % sur le jeu de test |
| O5 | Visualisation | ≥ 4 graphiques commentés dans un notebook |
| O6 | Reproductibilité | `git clone` + `docker compose up` suffit à lancer le projet |

## 2. Périmètre

**Inclus** : collecte via l'API Mastodon, Kafka, Spark Structured Streaming, Spark batch, PostgreSQL, MLlib, notebooks Jupyter, Docker.

**Exclus** : interface web de production, déploiement cloud, haute disponibilité multi-nœuds, géolocalisation précise (non fournie par Mastodon).

## 3. Architecture

```
 ┌──────────────┐   API / streaming   ┌───────────────┐   topic: mastodon_stream
 │ Mastodon API │ ──────────────────▶ │  Producer Py  │ ─────────────────────────┐
 └──────────────┘    (Mastodon.py)    └───────────────┘                          ▼
                                                                         ┌──────────────┐
                                                                         │    Kafka     │
                                                                         └──────┬───────┘
                                                                                │
                                        Spark Structured Streaming  ◀───────────┘
                                        (nettoyage, filtres, fenêtres, agrégats)
                                                     │ foreachBatch / JDBC
                                                     ▼
 ┌────────────────────┐   JDBC   ┌──────────────────────────────┐
 │ Spark Batch + MLlib│ ◀──────▶ │          PostgreSQL          │
 │ (analyses, modèle) │          │ toots, agrégats, sentiments  │
 └────────────────────┘          └──────────────┬───────────────┘
                                                │
                                                ▼
                                   Jupyter (Matplotlib / Seaborn)
```

### 3.1 Stack technique

| Composant | Choix | Justification |
|---|---|---|
| Collecte | Python 3.13 (géré par `uv`), `Mastodon.py`, `kafka-python` | Bibliothèque officielle recommandée |
| Messagerie | Apache Kafka 4.3 (mode KRaft) | Pas de Zookeeper → setup simplifié |
| Traitement | Apache Spark 4.1.2 (PySpark), cluster standalone 1 master + 1 worker | Streaming + batch + MLlib dans un seul moteur ; même image (Python 3.13, Java 21) pour driver et executors |
| Stockage | PostgreSQL 16 | Imposé par l'énoncé, accès JDBC |
| ML | Spark MLlib | Imposé par l'énoncé |
| Visualisation | Jupyter, Matplotlib, Seaborn, pandas | Imposé par l'énoncé |
| Orchestration | Docker Compose | Reproductibilité |

## 4. Exigences fonctionnelles

### EF1 — Ingestion (Mastodon → Kafka)
- EF1.1 Authentification OAuth via une application Mastodon enregistrée ; secrets dans `.env` (jamais commités).
- EF1.2 Filtrage sur une liste configurable de hashtags/mots-clés (par défaut : `#DataScience`, `#AI`, `#MachineLearning`, `#Python`, `#BigData`).
- EF1.3 Deux modes : **streaming** (WebSocket Mastodon, une connexion pour tous les hashtags, rattrapage REST après chaque reconnexion ; nécessite un access token de portée `read:statuses`) et **polling** de secours (`timeline_hashtag` + pagination `min_id`, sans token), avec respect de la limite de 300 requêtes / 5 min. Option de **backfill** au démarrage pour constituer l'historique.
- EF1.4 Chaque message Kafka est un JSON structuré :

```json
{
  "toot_id": "113...", "created_at": "2026-10-09T13:02:11Z",
  "user_id": "1094...", "username": "alice@mastodon.social",
  "language": "en", "content": "texte nettoyé (HTML retiré)",
  "hashtags": ["ai", "datascience"],
  "favourites_count": 3, "reblogs_count": 1, "replies_count": 0,
  "location": null, "source_tag": "ai", "ingested_at": "2026-10-09T13:02:12Z"
}
```
- EF1.5 Clé Kafka = `toot_id` ; topic `mastodon_stream` (3 partitions).
- EF1.6 Reconnexion automatique et journalisation des erreurs.

### EF2 — Stream processing (Spark Structured Streaming)
- EF2.1 Lecture du topic Kafka et parsing avec un schéma explicite.
- EF2.2 Nettoyage : texte vide, URLs, mentions (la longueur d'un toot est mesurée sur le texte nettoyé), doublons (`dropDuplicatesWithinWatermark` côté Spark, `ON CONFLICT` côté PostgreSQL).
- EF2.3 **Transformation 1** : filtre par langue (défaut : `en`) et/ou mot-clé, appliqué aux agrégats. La table brute `toots` conserve toutes les langues (source de vérité pour le batch).
- EF2.4 **Transformation 2** : fenêtres temporelles sur `created_at` (1 min pour la démo, 1 h pour l'analyse), watermark 1 h : les toots fédérés arrivent souvent avec plusieurs dizaines de minutes de retard. Les toots plus anciens (backfill) sont exclus des agrégats temps réel mais traités par le batch.
- EF2.5 **Action 1** : nombre de toots par fenêtre.
- EF2.6 **Action 2** : longueur moyenne des toots par utilisateur et par hashtag.
- EF2.7 Écriture dans PostgreSQL via `foreachBatch` → table de staging JDBC → `INSERT … ON CONFLICT` (idempotent), avec checkpoint par requête.

### EF3 — Batch processing
- EF3.1 Lecture des données historiques depuis PostgreSQL (JDBC partitionné : `partitionColumn`, `numPartitions`).
- EF3.2 Filtre des utilisateurs actifs (> X toots, X paramétrable).
- EF3.3 Regroupement par date et hashtag, comptage, hashtag le plus fréquent (global et par jour).
- EF3.4 Agrégats : toots / jour, longueur moyenne, engagement moyen.
- EF3.5 **Optimisation** : comparer au moins 3 scénarios (baseline, `cache`, `repartition`/`coalesce`) avec temps d'exécution et captures Spark UI.
- EF3.6 (Optionnel) Enrichissement avec un dataset Twitter Kaggle.

### EF4 — Analyse de sentiment (MLlib)
- EF4.1 Jeu d'entraînement : **Sentiment140** (labels 0 = négatif, 4 = positif → 0/1).
- EF4.2 Pipeline : nettoyage → `RegexTokenizer` → `StopWordsRemover` → `CountVectorizer` ou `HashingTF` + `IDF` → classifieur.
- EF4.3 Comparaison `LogisticRegression` vs `NaiveBayes`.
- EF4.4 Split 80/20 (seed fixe) ; métriques : accuracy, F1, AUC-ROC, matrice de confusion.
- EF4.5 Sauvegarde du `PipelineModel`.
- EF4.6 Application aux toots historiques (anglais) ; classe **neutre** si la probabilité positive ∈ [0,4 ; 0,6].
- EF4.7 Stockage du résultat dans `toot_sentiment`.

### EF5 — Visualisation
- EF5.1 Distribution des sentiments (bar chart).
- EF5.2 Volume de toots dans le temps (line chart).
- EF5.3 Top hashtags (horizontal bar chart).
- EF5.4 Heatmap d'activité (jour de semaine × heure).
- EF5.5 Chaque graphique est accompagné d'une interprétation.

## 5. Exigences non fonctionnelles

| Code | Exigence |
|---|---|
| ENF1 | **Reproductibilité** : tout le projet démarre avec `docker compose up -d` |
| ENF2 | **Sécurité** : aucun secret dans le dépôt (`.env` + `.env.example`) |
| ENF3 | **Tolérance aux pannes** : checkpoints Spark, reconnexion du producer |
| ENF4 | **Respect de l'API** : limites de débit Mastodon, User-Agent explicite |
| ENF5 | **Qualité du code** : docstrings, commentaires sur les étapes clés, PEP 8 |
| ENF6 | **Ressources** : fonctionne sur un poste avec 8 Go de RAM alloués à Docker |
| ENF7 | **Éthique / RGPD** : données publiques uniquement, usage pédagogique |

## 6. Modèle de données (PostgreSQL)

| Table | Contenu | Alimentée par |
|---|---|---|
| `toots` | `toot_id` (PK), `created_at`, `user_id`, `username`, `language`, `content`, `content_length`, `favourites_count`, `reblogs_count`, `ingested_at` | Streaming |
| `toot_hashtags` | `toot_id`, `hashtag` (PK composite) | Streaming |
| `stream_window_counts` | `window_start`, `window_end`, `granularity`, `toot_count` | Streaming (A1) |
| `stream_user_stats` | `window_start`, `user_id`, `toot_count`, `avg_length` | Streaming (A2) |
| `stream_hashtag_stats` | `window_start`, `hashtag`, `toot_count`, `avg_length` | Streaming (A2) |
| `batch_daily_stats` | `day`, `toot_count`, `avg_length`, `avg_engagement`, `top_hashtag` | Batch |
| `toot_sentiment` | `toot_id` (PK), `content`, `sentiment` (`positive`/`negative`/`neutral`), `probability`, `model_name`, `scored_at` | MLlib |

## 7. Livrables

1. Dépôt Git complet : code, `docker-compose.yml`, scripts SQL, notebooks.
2. `README.md` : architecture, prérequis, installation, lancement, démo.
3. Notebooks : batch + optimisation, ML, visualisations.
4. Captures Spark UI et tableau comparatif des performances.
5. Présentation (slides) + démo live du flux arrivant dans PostgreSQL.

## 8. Planning indicatif

| Semaine | Lots |
|---|---|
| S1 | Phase 0 + 1 (environnement, Docker) + début ingestion |
| S2 | Ingestion finalisée, lancement de la collecte continue, streaming Spark |
| S3 | Batch + optimisation, entraînement du modèle de sentiment |
| S4 | Scoring, visualisations, README, slides, répétition de la démo |

> Lancer la collecte **le plus tôt possible** : le volume de données historiques dépend de la durée d'ingestion.

## 9. Risques et parades

| Risque | Impact | Parade |
|---|---|---|
| Streaming API bloquée ou instable sur l'instance | Pas de données | Mode polling + changement d'instance (`fosstodon.org`, `mastodon.social`) |
| Rate limit (300 req / 5 min) | Collecte ralentie | Polling espacé, lecture des en-têtes `X-RateLimit-*` |
| Volume trop faible pour le batch | Analyses peu significatives | Collecte continue + dataset Kaggle en complément |
| Domaine différent (tweets 2009 vs toots tech) | Sentiment biaisé | Le mentionner dans la soutenance, inspecter des exemples, seuil neutre |
| Toots multilingues | Modèle anglais inadapté | Score uniquement sur `language = 'en'` |
| RAM insuffisante (Spark + Kafka + Postgres) | Crashs | Limiter la mémoire des executors, 1 seul worker |
| Démo live qui échoue | Soutenance pénalisée | Vidéo de secours + données pré-chargées |

## 10. Points à clarifier

- **Langue de la soutenance** : l'énoncé mentionne le japonais (objectifs) **et** l'anglais (livrables) — à confirmer avec l'enseignant.
- Valeur du seuil X pour les « utilisateurs actifs ».
- Liste définitive des hashtags suivis.
- Taille de l'équipe et répartition des lots.

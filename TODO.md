# TODO — Projet Mastodon × Spark

Légende : `[ ]` à faire · `[~]` en cours · `[x]` terminé
Voir le [cahier des charges](CAHIER_DES_CHARGES.md) pour le détail de chaque exigence.

---

## Phase 0 — Cadrage & environnement
- [x] Dépendances Python locales avec `uv` (`uv sync`)
- [ ] Valider le cahier des charges avec l'équipe (répartition des rôles)
- [x] Initialiser le dépôt Git + `.gitignore` (`.env`, `data/`, `checkpoints/`, `__pycache__/`, `.ipynb_checkpoints/`)
- [ ] Créer l'arborescence du projet (cf. CDC §8)
- [ ] Créer un compte Mastodon (ex. `mastodon.social`) et enregistrer une application → `client_id`, `client_secret`, `access_token`
- [x] Créer `.env.example` (instance, token, identifiants Postgres, hashtags suivis)
- [x] Vérifier Docker Desktop (WSL2) : RAM ≥ 8 Go allouée

## Phase 1 — Infrastructure Docker
- [x] `docker-compose.yml` : Kafka (mode KRaft, sans Zookeeper), PostgreSQL 16, Spark 4.1.2 (master + worker + history server), Jupyter, Kafka UI
- [x] Script d'init Postgres `sql/init.sql` (création des tables, cf. CDC §6)
- [x] Création automatique du topic `mastodon_stream` (3 partitions)
- [x] Ajouter les JARs : `spark-sql-kafka-0-10`, driver JDBC `postgresql` (intégrés à l'image `docker/spark/Dockerfile`)
- [x] Test de fumée : Spark → Kafka (batch + streaming) → PostgreSQL (JDBC) sur le cluster

## Phase 2 — Ingestion (Mastodon → Kafka)
- [ ] `producer/mastodon_producer.py` avec Mastodon.py + `kafka-python` (ou `confluent-kafka`)
- [ ] Mode streaming : `stream_hashtag()` / `stream_public()` filtré sur les mots-clés
- [ ] Mode secours : polling `timeline_hashtag()` avec `since_id` (respect 300 req / 5 min)
- [ ] Parsing : `id`, `created_at`, `content` (HTML → texte brut), `account.id`, `account.acct`, `language`, `tags`, `favourites_count`, `reblogs_count`, `replies_count`, `location` (null — non fourni par Mastodon)
- [ ] Sérialisation JSON, clé Kafka = `toot_id` (dédoublonnage)
- [ ] Gestion des erreurs / reconnexion / logs
- [ ] Vérifier la réception avec `kafka-console-consumer` ou Kafka UI

## Phase 3 — Stream processing (Spark Structured Streaming)
- [ ] `streaming/stream_job.py` : lecture Kafka → schéma explicite (`StructType`) → DataFrame
- [ ] Nettoyage : suppression URLs/mentions, texte vide, doublons (`dropDuplicates` + watermark)
- [ ] T1 : filtre par langue (`en` / `fr`) et/ou mots-clés
- [ ] T2 : fenêtres temporelles (`window(created_at, "1 minute")` et `"1 hour"`) + watermark 10 min
- [ ] A1 : nombre de toots par fenêtre
- [ ] A2 : longueur moyenne des toots par utilisateur et par hashtag
- [ ] Sink : `foreachBatch` → JDBC PostgreSQL (toots bruts + agrégats)
- [ ] Checkpoints pour la reprise après crash
- [ ] Démo : requête SQL qui montre les lignes qui arrivent en temps réel

## Phase 4 — Batch processing (PostgreSQL → Spark)
- [ ] Laisser tourner l'ingestion plusieurs jours (objectif ≥ 10 000 toots)
- [ ] (Optionnel) Compléter avec un dataset Kaggle Twitter
- [ ] `batch/batch_job.py` (ou notebook) : lecture JDBC partitionnée
- [ ] Filtre utilisateurs actifs (> X toots)
- [ ] Group by jour × hashtag, total, hashtag le plus fréquent
- [ ] Toots par jour, longueur moyenne, engagement moyen (favs + reblogs)
- [ ] Optimisation : mesures **avant/après** `repartition`, `coalesce`, `cache`/`persist`
- [ ] Captures Spark UI (jobs, stages, DAG, storage) + tableau comparatif des temps

## Phase 5 — Sentiment analysis (Spark MLlib)
- [ ] Télécharger Sentiment140 (1,6 M tweets, labels 0/4 → 0/1)
- [ ] Pipeline : nettoyage → `Tokenizer`/`RegexTokenizer` → `StopWordsRemover` → `CountVectorizer`/`HashingTF` + `IDF`
- [ ] Modèles : `LogisticRegression` vs `NaiveBayes`
- [ ] Split 80/20, évaluation : accuracy, F1, AUC, matrice de confusion
- [ ] (Bonus) `CrossValidator` / `ParamGridBuilder`
- [ ] Sauvegarde du `PipelineModel` dans `models/`
- [ ] Application sur les toots historiques → positif / négatif / **neutre** (seuil de probabilité)
- [ ] Écriture dans la table `toot_sentiment`

## Phase 6 — Visualisation (Jupyter)
- [ ] Distribution des sentiments (bar chart)
- [ ] Fréquence des toots dans le temps (line chart)
- [ ] Top 15 hashtags (horizontal bar)
- [ ] Heatmap activité (jour × heure)
- [ ] (Bonus) sentiment par hashtag, évolution du sentiment dans le temps
- [ ] Rédiger les insights sous chaque graphique

## Phase 7 — Livrables
- [ ] `README.md` : architecture, prérequis, installation pas à pas, lancement, démo
- [ ] Commentaires dans le code (docstrings + explication des étapes clés)
- [ ] Schéma d'architecture (image)
- [ ] Slides de présentation (architecture, choix, difficultés, résultats, démo)
- [ ] Répétition de la démo live (plan B : vidéo enregistrée)
- [ ] Clarifier la langue de soutenance (énoncé : japonais **et** anglais — à confirmer)
- [ ] Dernière vérif : clone propre → `docker compose up` → tout fonctionne

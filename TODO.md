# TODO — Projet Mastodon × Spark

Légende : `[ ]` à faire · `[~]` en cours · `[x]` terminé
Voir le [cahier des charges](CAHIER_DES_CHARGES.md) pour le détail de chaque exigence.

---

## Phase 0 — Cadrage & environnement
- [x] Dépendances Python locales avec `uv` (`uv sync`)
- [x] Valider le cahier des charges avec l'équipe (répartition des rôles)
- [x] Initialiser le dépôt Git + `.gitignore` (`.env`, `data/`, `checkpoints/`, `__pycache__/`, `.ipynb_checkpoints/`)
- [x] Créer l'arborescence du projet
- [x] Créer un compte Mastodon (ex. `mastodon.social`) et enregistrer une application → `client_id`, `client_secret`, `access_token`
- [x] Créer `.env.example` (instance, token, identifiants Postgres, hashtags suivis)
- [x] Vérifier Docker Desktop (WSL2) : RAM ≥ 8 Go allouée

## Phase 1 — Infrastructure Docker
- [x] `docker-compose.yml` : Kafka (mode KRaft, sans Zookeeper), PostgreSQL 16, Spark 4.1.2 (master + worker + history server), Jupyter, Kafka UI
- [x] Script d'init Postgres `sql/init.sql` (création des tables, cf. CDC §6)
- [x] Création automatique du topic `mastodon_stream` (3 partitions)
- [x] Ajouter les JARs : `spark-sql-kafka-0-10`, driver JDBC `postgresql` (intégrés à l'image `docker/spark/Dockerfile`)
- [x] Test de fumée : Spark → Kafka (batch + streaming) → PostgreSQL (JDBC) sur le cluster

## Phase 2 — Ingestion (Mastodon → Kafka)
- [x] `producer/mastodon_producer.py` avec `requests` + `websockets` + `kafka-python` (+ `toot_parser.py`, image Docker `producer`). Mastodon.py abandonné : streaming SSE coupé par mastodon.social, et conversion typée des réponses trop lente (~20 s par page de 40 toots)
- [x] Liste de hashtags élargie : 74 hashtags tech/data (~1 400 toots/h), cf. CDC EF1.2
- [x] Mode streaming : WebSocket Mastodon (une connexion, tous les hashtags) + rattrapage REST à chaque reconnexion. `stream_hashtag()` de Mastodon.py abandonné : mastodon.social coupe le streaming HTTP (SSE) toutes les ~16 s
- [ ] Régénérer l'access token Mastodon (il a circulé en clair)
- [x] Mode secours : polling `timeline_hashtag()` avec `min_id` (aucun toot perdu) + backfill historique ; limite 300 req / 5 min gérée par `ratelimit_method="pace"`
- [x] Parsing : `id`, `created_at`, `content` (HTML → texte brut), `account.id`, `account.acct`, `language`, `tags`, `favourites_count`, `reblogs_count`, `replies_count`, `location` (null — non fourni par Mastodon)
- [x] Sérialisation JSON, clé Kafka = `toot_id` (dédoublonnage côté producer ; à refaire côté Spark/Postgres)
- [x] Gestion des erreurs / reconnexion / logs / arrêt propre (SIGTERM)
- [x] Vérifier la réception avec `kafka-console-consumer` ou Kafka UI

## Phase 3 — Stream processing (Spark Structured Streaming)
- [x] `streaming/stream_job.py` : lecture Kafka → schéma explicite (`StructType`) → DataFrame (service Docker `stream-job`)
- [x] Nettoyage : suppression URLs/mentions, texte vide, doublons (`dropDuplicatesWithinWatermark` + upsert `ON CONFLICT`)
- [x] T1 : filtre par langue (`STREAM_LANGUAGES`, défaut `en` = 84 % des toots) et mots-clés optionnels (`STREAM_KEYWORDS`), appliqué aux agrégats
- [x] T2 : fenêtres temporelles (`window(created_at, "1 minute")` et `"1 hour"`) + watermark 1 h (toots fédérés en retard)
- [x] A1 : nombre de toots par fenêtre (`stream_window_counts`)
- [x] A2 : longueur moyenne des toots par utilisateur et par hashtag (`stream_user_stats`, `stream_hashtag_stats`)
- [x] Sink : `foreachBatch` → JDBC staging → `INSERT … ON CONFLICT` (idempotent)
- [x] Checkpoints pour la reprise après crash (testé : 2 redémarrages, compteurs exacts)
- [x] Démo : `sql/demo.sql` (avec `\watch 5` dans psql)

## Phase 4 — Batch processing (PostgreSQL → Spark)
- [ ] Laisser tourner l'ingestion plusieurs jours (objectif ≥ 10 000 toots)
- [ ] (Optionnel) Compléter avec un dataset Kaggle Twitter
- [x] `batch/batch_job.py` : lecture JDBC partitionnée (4 partitions sur un hash de `toot_id`, colonnes utiles uniquement)
- [x] Filtre utilisateurs actifs (> `BATCH_ACTIVE_THRESHOLD` toots, défaut 5) → `batch_active_users` (+ hashtag favori)
- [x] Group by jour × hashtag avec rang dans la journée (`batch_hashtag_daily`), totaux par hashtag (`batch_hashtag_totals`), hashtag le plus fréquent global et par jour
- [x] Toots par jour, longueur moyenne, engagement moyen (favs + reblogs), hashtag du jour → `batch_daily_stats`
- [x] Constat : un compte relais (`hackaday@www.urbanmind.net`) publie ~15 % des toots avec toujours les mêmes hashtags (#engineering, #community, #inclusive…) → à signaler dans l'analyse
- [x] Optimisation : mesures **avant/après** `repartition`, `coalesce`, `cache`/`persist` (`--scenario`, `batch/benchmark.sh`, table `batch_benchmarks`) : 29,0 s → 20,8 s (calculs 27,0 s → 13,4 s), cf. README
- [ ] Captures Spark UI (jobs, stages, DAG, storage) + tableau comparatif des temps

## Phase 5 — Sentiment analysis (Spark MLlib)
- [x] Télécharger Sentiment140 (1,6 M tweets, labels 0/4 → 0/1) — depuis Stanford, sans compte Kaggle (cf. README)
- [x] Pipeline (`ml/train_sentiment.py`) : nettoyage (`SQLTransformer`) → `RegexTokenizer` → `StopWordsRemover` (négations conservées) → `CountVectorizer` (46 737 mots) + `IDF`
- [x] Modèles : `LogisticRegression` (exactitude 78,6 %, AUC 0,857) vs `NaiveBayes` (76,2 %, AUC 0,828) → régression logistique retenue
- [x] Split 80/20, évaluation : accuracy, F1, AUC, matrice de confusion → table `sentiment_model_metrics`
- [ ] (Bonus) `CrossValidator` / `ParamGridBuilder`
- [x] Sauvegarde du `PipelineModel` (nettoyage compris) dans `models/sentiment/`
- [x] Seuil neutre choisi sur le jeu annoté à la main (498 tweets dont 139 neutres) : neutre si |p(positif) − 0,5| ≤ 0,20
- [x] Application sur les toots historiques en anglais (`ml/score_toots.py`) → positif / négatif / **neutre** (seuil de probabilité)
- [x] Écriture dans la table `toot_sentiment` (09/10 : 9 280 toots, 54 % positifs, 25 % neutres, 21 % négatifs)
- [x] Constat : biais de domaine (tweets perso 2009 vs toots tech) — vocabulaire technique « error », « missing », « vulnerability » tiré vers le négatif, alertes automatiques parfois positives → à présenter

## Phase 6 — Visualisation (Jupyter)
- [x] Notebook `notebooks/visualisations.ipynb` (exécuté, figures PNG dans `notebooks/figures/` pour les slides)
- [x] Distribution des sentiments (bar chart)
- [x] Fréquence des toots dans le temps (line chart, avec la limite backfill / collecte)
- [x] Top 15 hashtags (horizontal bar, hashtags dominés par un seul compte signalés)
- [x] Heatmap activité (jour × heure)
- [x] (Bonus) sentiment par hashtag, temps d'optimisation du batch, matrice de confusion du modèle
- [x] Rédiger les insights sous chaque graphique

## Phase 7 — Livrables
- [x] `README.md` : architecture, prérequis, installation pas à pas, lancement, démo
- [x] Commentaires dans le code (docstrings + explication des étapes clés)
- [x] Schéma d'architecture (diagramme Mermaid dans le README, rendu par GitHub)
- [x] `sql/init.sql` testé sur une base vide (12 tables)
- [ ] Slides de présentation (architecture, choix, difficultés, résultats, démo)
- [ ] Répétition de la démo live (plan B : vidéo enregistrée)
- [x] Langue de soutenance : **anglais** (slides et présentation orale)
- [ ] Dernière vérif : clone propre → `docker compose up` → tout fonctionne

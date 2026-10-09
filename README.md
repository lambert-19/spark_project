# Mastodon × Spark

Pipeline temps réel et batch sur les toots Mastodon : **Mastodon API → Kafka → Spark Structured Streaming → PostgreSQL**, puis analyses batch, sentiment (Spark MLlib) et visualisations dans Jupyter.

Spécifications : [CAHIER_DES_CHARGES.md](CAHIER_DES_CHARGES.md) · Avancement : [TODO.md](TODO.md)

## Prérequis

- Docker Desktop (≥ 8 Go de RAM alloués)
- [uv](https://docs.astral.sh/uv/) pour l'environnement Python local (producer, autocomplétion)

## Démarrage

```bash
cp .env.example .env          # puis renseigner les identifiants Mastodon
uv sync                       # dépendances Python locales
docker compose up -d --build  # infrastructure
```

Le premier build télécharge l'image Spark/Jupyter (~4 Go).

### Services et ports

Tous les ports sont publiés sur `127.0.0.1` uniquement (pas d'accès depuis le réseau local). Entre conteneurs, on utilise le nom du service et le port interne.

| Service | Port hôte | Port interne (conteneurs) | Accès depuis le PC | Rôle |
|---|---|---|---|---|
| `jupyter` | 8888 | `jupyter:8888` | http://localhost:8888 (token : `JUPYTER_TOKEN`) | Notebooks, driver Spark |
| `jupyter` | 4040, 4041 | `jupyter:4040-4041` | http://localhost:4040 | Spark UI des applications lancées depuis Jupyter (4041 pour une 2ᵉ en parallèle) |
| `stream-job` | 4042 | `stream-job:4040` | http://localhost:4042 | Spark UI du job de streaming |
| `spark-master` | 8080 | `spark-master:8080` | http://localhost:8080 | Interface du cluster Spark |
| `spark-master` | 7077 | `spark-master:7077` | `spark://127.0.0.1:7077` | Soumission des applications Spark |
| `spark-worker` | 8081 | `spark-worker:8081` | http://localhost:8081 | Interface du worker (4 cœurs, 3 Go) |
| `spark-history` | 18080 | `spark-history:18080` | http://localhost:18080 | Applications terminées (comparaison des optimisations) |
| `kafka` | 9094 | `kafka:9092` | `127.0.0.1:9094` (pas `localhost`, cf. ci-dessous) | Broker Kafka, topic `mastodon_stream` |
| `kafka` | — | `kafka:9093` | — | Contrôleur KRaft (interne) |
| `kafka-ui` | 8082 | `kafka-ui:8080` | http://localhost:8082 | Topics et messages Kafka |
| `postgres` | 5433 (`POSTGRES_HOST_PORT`) | `postgres:5432` | `localhost:5433` | Base `mastodon` |
| `producer` | — | — | `docker compose logs -f producer` | Collecte Mastodon → Kafka |
| `kafka-init` | — | — | — | Crée le topic au démarrage puis s'arrête |

- **PostgreSQL sur 5433** côté PC : le 5432 est souvent déjà pris par un PostgreSQL installé localement.
- **Kafka via `127.0.0.1`** : kafka-python ne retente pas en IPv4 quand `localhost` échoue en IPv6.

## Architecture Docker

- **Une seule image Spark** ([docker/spark/Dockerfile](docker/spark/Dockerfile)) pour le master, le worker, le History Server et Jupyter : driver et executors ont les mêmes versions (Spark 4.1.2, Python 3.13, Java 21). Les JARs Kafka et PostgreSQL sont intégrés à l'image.
- Le dossier du projet est monté sur `/home/jovyan/work` dans tous les conteneurs Spark, pour que driver et executors voient les mêmes chemins (datasets, modèles, checkpoints).
- Chaque application Spark est limitée à 2 cœurs ([spark-defaults.conf](docker/spark/spark-defaults.conf)) : le job de streaming et un notebook peuvent tourner en même temps.
- Le schéma PostgreSQL ([sql/init.sql](sql/init.sql)) n'est appliqué qu'au **premier** démarrage. Pour le réappliquer : `docker compose down -v` (⚠ supprime les données).

## Collecte des toots (producer)

Le service `producer` démarre avec la stack et publie les toots dans le topic `mastodon_stream`.

- **Sans token** : mode polling (interrogation toutes les `PRODUCER_POLL_INTERVAL` secondes).
- **Avec `MASTODON_ACCESS_TOKEN`** dans `.env` : mode streaming temps réel par WebSocket. Créer une application dans *Préférences → Développement → Nouvelle application* (portée `read:statuses` uniquement), copier « Your access token » dans `.env`, puis `docker compose up -d --force-recreate producer`.
- Au démarrage, `PRODUCER_BACKFILL_PAGES` pages d'historique (40 toots) sont récupérées par hashtag.

```bash
docker compose logs -f producer                                  # suivre la collecte
docker compose stop producer                                     # arrêter la collecte
uv run producer/mastodon_producer.py --mode poll --duration 60   # lancement local (test)
```

## Job de streaming (Kafka → PostgreSQL)

Le service `stream-job` lance [streaming/stream_job.py](streaming/stream_job.py) au démarrage de la stack. Il exécute 5 requêtes Structured Streaming :

| Requête | Table PostgreSQL | Contenu |
|---|---|---|
| `toots` | `toots`, `toot_hashtags` | Couche brute, toutes langues, dédoublonnée |
| `counts_1min` / `counts_1hour` | `stream_window_counts` | Toots par fenêtre (Action 1) |
| `user_stats` | `stream_user_stats` | Toots et longueur moyenne par utilisateur et par heure (Action 2) |
| `hashtag_stats` | `stream_hashtag_stats` | Toots et longueur moyenne par hashtag et par heure (Action 2) |

Les agrégats ne portent que sur les langues de `STREAM_LANGUAGES` (défaut `en`). Les tables `stg_*` sont des tables de staging techniques (écriture idempotente).

```bash
docker compose logs -f stream-job                                              # suivi
docker compose exec postgres psql -U mastodon -d mastodon -f /dev/stdin < sql/demo.sql   # démo
```

> Changer les requêtes (agrégations, `spark.sql.shuffle.partitions`) rend les checkpoints incompatibles : arrêter le job et supprimer `checkpoints/stream_job/` avant de relancer.

## Job batch (PostgreSQL → Spark → PostgreSQL)

[batch/batch_job.py](batch/batch_job.py) analyse tout l'historique des tables `toots` et `toot_hashtags` (lecture JDBC partitionnée) et recalcule entièrement les tables de résultats à chaque exécution :

| Table | Contenu |
|---|---|
| `batch_active_users` | Utilisateurs actifs (plus de `BATCH_ACTIVE_THRESHOLD` toots, défaut 5) : volume, longueur et engagement moyens, hashtag favori |
| `batch_hashtag_daily` | Toots par jour et par hashtag ; `rank = 1` pour le hashtag le plus fréquent du jour |
| `batch_hashtag_totals` | Totaux par hashtag sur tout l'historique |
| `batch_daily_stats` | Toots par jour, longueur moyenne, engagement moyen (favourites + reblogs), hashtag du jour |

```bash
docker compose exec jupyter spark-submit /home/jovyan/work/batch/batch_job.py
```

### Optimisation

L'option `--scenario` active les optimisations une par une, pour mesurer leur effet :

| Scénario | Optimisation |
|---|---|
| `baseline` | Aucune : chacune des 4 sorties relit PostgreSQL et recalcule tout le plan |
| `cache` | `toots` et `toot_hashtags` lus une seule fois puis gardés en mémoire (`persist`) |
| `repartition` | Les deux tables sont repartitionnées sur `toot_id` (4 partitions, et `spark.sql.shuffle.partitions = 4` au lieu de 200) : les jointures n'ajoutent plus de shuffle ; `coalesce(1)` avant l'écriture des résultats |
| `optimized` | `cache` + `repartition` (scénario par défaut) |

```bash
batch/benchmark.sh 3    # chaque scénario 3 fois, sur les mêmes données ; médianes affichées à la fin
```

Chaque exécution ajoute une ligne (durée de chaque étape, `app_id`) dans la table `batch_benchmarks` et apparaît dans le Spark History Server (http://localhost:18080), sous le nom `mastodon-batch-job [scénario]`.

Résultats du 09/10/2026 (12 079 toots, médiane de 3 exécutions, en secondes) :

| Scénario | Lecture | active_users | hashtag_daily | hashtag_totals | daily_stats | Calculs (hors lecture) | **Total** |
|---|---|---|---|---|---|---|---|
| `baseline` | 2.3 | 13.1 | 3.6 | 3.5 | 4.4 | 27.0 | **29.0** |
| `cache` | 7.5 | 7.5 | 2.0 | 2.5 | 1.6 | 13.8 | **21.3** |
| `repartition` | 2.4 | 12.1 | 2.2 | 2.3 | 2.0 | 18.6 | **21.0** |
| `optimized` | 7.3 | 7.5 | 1.9 | 2.0 | 1.5 | 13.4 | **20.8** |

- **cache** : la lecture coûte plus cher (matérialisation du cache), mais les 4 sorties ne relisent plus PostgreSQL : temps de calcul divisé par 2.
- **repartition** : 4 partitions de shuffle au lieu de 200 et jointures sans redistribution ; gain surtout sur les petites sorties (hashtag_daily, daily_stats).
- Le volume est petit : une part importante du temps est un coût fixe (démarrage des tâches, connexions JDBC, première exécution de `active_users`), ce qui limite le gain total à environ 28 %.

> Base créée avant l'ajout de ces tables : appliquer le schéma (idempotent, `IF NOT EXISTS`) avec
> `docker compose exec -T postgres psql -U mastodon -d mastodon -f /dev/stdin < sql/init.sql`.

## Lancer un job Spark

```bash
docker compose exec jupyter spark-submit /home/jovyan/work/<chemin/du/job.py>
```

> `docker compose exec jupyter python ...` ne trouve pas `pyspark` : utiliser `spark-submit`, ou un notebook.

## Commandes utiles

```bash
docker compose ps                     # état des services
docker compose logs -f <service>      # logs
docker compose down                   # arrêt (données conservées)
docker compose exec postgres psql -U mastodon -d mastodon
```

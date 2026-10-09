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

| Service | URL | Rôle |
|---|---|---|
| Jupyter | http://localhost:8888 (token : `JUPYTER_TOKEN`) | Notebooks, driver Spark |
| Spark Master | http://localhost:8080 | État du cluster |
| Spark Worker | http://localhost:8081 | 4 cœurs, 3 Go |
| Spark UI | http://localhost:4040 | Application en cours |
| Spark History | http://localhost:18080 | Applications terminées (comparaison des optimisations) |
| Kafka UI | http://localhost:8082 | Topics et messages |
| Kafka | `localhost:9094` (hôte) · `kafka:9092` (conteneurs) | Topic `mastodon_stream` |
| PostgreSQL | `localhost:5433` (hôte) · `postgres:5432` (conteneurs) | Base `mastodon` |

## Architecture Docker

- **Une seule image Spark** ([docker/spark/Dockerfile](docker/spark/Dockerfile)) pour le master, le worker, le History Server et Jupyter : driver et executors ont les mêmes versions (Spark 4.1.2, Python 3.13, Java 21). Les JARs Kafka et PostgreSQL sont intégrés à l'image.
- Le dossier du projet est monté sur `/home/jovyan/work` dans tous les conteneurs Spark, pour que driver et executors voient les mêmes chemins (datasets, modèles, checkpoints).
- Chaque application Spark est limitée à 2 cœurs ([spark-defaults.conf](docker/spark/spark-defaults.conf)) : le job de streaming et un notebook peuvent tourner en même temps.
- Le schéma PostgreSQL ([sql/init.sql](sql/init.sql)) n'est appliqué qu'au **premier** démarrage. Pour le réappliquer : `docker compose down -v` (⚠ supprime les données).

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

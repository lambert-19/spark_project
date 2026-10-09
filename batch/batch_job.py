"""Job Spark batch : analyse de l'historique des toots stockés dans PostgreSQL (partie 3).

Lit les tables `toots` et `toot_hashtags` (alimentées par le job de streaming) par JDBC partitionné,
puis calcule :

    batch_active_users     utilisateurs actifs (plus de BATCH_ACTIVE_THRESHOLD toots) : volume,
                           longueur et engagement moyens, hashtag favori
    batch_hashtag_daily    nombre de toots par jour et par hashtag, rang du hashtag dans la journée
                           (rank = 1 : hashtag le plus fréquent du jour)
    batch_hashtag_totals   totaux par hashtag sur tout l'historique (hashtag le plus fréquent global)
    batch_daily_stats      toots par jour, longueur moyenne, engagement moyen, hashtag du jour

Toutes les langues sont prises en compte (contrairement aux agrégats temps réel, filtrés sur
STREAM_LANGUAGES). L'engagement d'un toot est favourites + reblogs, tel que relevé lors de sa
dernière ingestion : les toots récents ont mécaniquement moins d'engagement.

Les tables de résultats sont recalculées entièrement à chaque exécution (vidées puis remplies) :
relancer le job est sans risque.

Lancement :
    docker compose exec jupyter spark-submit /home/jovyan/work/batch/batch_job.py
"""

import logging
import os
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

log = logging.getLogger("batch_job")

# ---------------------------------------------------------------- configuration

ACTIVE_THRESHOLD = int(os.environ.get("BATCH_ACTIVE_THRESHOLD", "5"))  # actif = plus de X toots
JDBC_PARTITIONS = int(os.environ.get("BATCH_JDBC_PARTITIONS", "4"))

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_INTERNAL_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "mastodon")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {
    "user": os.environ.get("POSTGRES_USER", "mastodon"),
    "password": os.environ.get("POSTGRES_PASSWORD", "mastodon"),
    "driver": "org.postgresql.Driver",
}


# ---------------------------------------------------------------- lecture PostgreSQL

def read_jdbc(spark: SparkSession, query: str) -> DataFrame:
    """Lecture JDBC partitionnée d'une requête SQL qui renvoie une colonne `toot_id`.

    Spark ouvre JDBC_PARTITIONS connexions en parallèle ; chacune lit les lignes dont la colonne
    calculée `part` (hash du toot_id) tombe dans sa plage. Un hash répartit les lignes de façon
    homogène, contrairement à created_at (quelques jours concentrent presque tous les toots).
    """
    n = JDBC_PARTITIONS
    dbtable = f"(SELECT q.*, mod(abs(hashtext(q.toot_id)::bigint), {n}) AS part FROM ({query}) q) t"
    return (
        spark.read.jdbc(JDBC_URL, dbtable, column="part", lowerBound=0, upperBound=n,
                        numPartitions=n, properties={**JDBC_PROPS, "fetchsize": "5000"})
        .drop("part")
    )


def load_toots(spark: SparkSession) -> DataFrame:
    """Toots historiques : seules les colonnes utiles au batch sont lues (pas le texte)."""
    toots = read_jdbc(spark, """
        SELECT toot_id, created_at, user_id, username, content_length,
               favourites_count, reblogs_count
        FROM toots
    """)
    return (
        toots.withColumn("day", F.to_date("created_at"))  # jour UTC (spark.sql.session.timeZone)
        .withColumn("engagement",
                    F.coalesce("favourites_count", F.lit(0)) + F.coalesce("reblogs_count", F.lit(0)))
    )


def load_hashtags(spark: SparkSession) -> DataFrame:
    return read_jdbc(spark, "SELECT toot_id, hashtag FROM toot_hashtags")


# ---------------------------------------------------------------- transformations

def top_by(df: DataFrame, partition_cols: list[str], order_col: str) -> DataFrame:
    """Ajoute `rank` : 1 pour la ligne au plus grand order_col de chaque groupe
    (égalités départagées par ordre alphabétique du hashtag)."""
    w = Window.partitionBy(*partition_cols).orderBy(F.desc(order_col), F.asc("hashtag"))
    return df.withColumn("rank", F.row_number().over(w))


def active_users(toots: DataFrame, hashtags: DataFrame) -> DataFrame:
    """Filtre des utilisateurs actifs (plus de ACTIVE_THRESHOLD toots) et leurs statistiques."""
    users = (
        toots.groupBy("user_id")
        .agg(F.max("username").alias("username"),
             F.count("*").alias("toot_count"),
             F.avg("content_length").alias("avg_length"),
             F.avg("engagement").alias("avg_engagement"),
             F.min("created_at").alias("first_toot"),
             F.max("created_at").alias("last_toot"))
        .where(F.col("toot_count") > ACTIVE_THRESHOLD)
    )
    # Hashtag favori de chaque utilisateur actif
    favourite = (
        toots.join(users.select("user_id"), "user_id")  # petite table -> broadcast automatique
        .join(hashtags, "toot_id")
        .groupBy("user_id", "hashtag").agg(F.count("*").alias("n"))
    )
    favourite = (
        top_by(favourite, ["user_id"], "n")
        .where("rank = 1")
        .select("user_id", F.col("hashtag").alias("top_hashtag"))
    )
    return users.join(favourite, "user_id", "left")


def hashtag_daily(toots: DataFrame, hashtags: DataFrame) -> DataFrame:
    """Regroupement par jour et par hashtag, avec le rang du hashtag dans la journée."""
    counts = (
        toots.select("toot_id", "day").join(hashtags, "toot_id")
        .groupBy("day", "hashtag").agg(F.count("*").alias("toot_count"))
    )
    return top_by(counts, ["day"], "toot_count")


def hashtag_totals(toots: DataFrame, hashtags: DataFrame) -> DataFrame:
    """Totaux par hashtag sur tout l'historique."""
    return (
        toots.join(hashtags, "toot_id")
        .groupBy("hashtag")
        .agg(F.count("*").alias("toot_count"),
             F.countDistinct("user_id").alias("user_count"),
             F.avg("content_length").alias("avg_length"),
             F.avg("engagement").alias("avg_engagement"))
    )


def daily_stats(toots: DataFrame, per_day_hashtags: DataFrame) -> DataFrame:
    """Toots par jour, longueur et engagement moyens, hashtag le plus fréquent du jour."""
    top = per_day_hashtags.where("rank = 1").select("day", F.col("hashtag").alias("top_hashtag"))
    return (
        toots.groupBy("day")
        .agg(F.count("*").alias("toot_count"),
             F.avg("content_length").alias("avg_length"),
             F.avg("engagement").alias("avg_engagement"))
        .join(top, "day", "left")
    )


# ---------------------------------------------------------------- écriture PostgreSQL

def overwrite(df: DataFrame, table: str) -> None:
    """Remplace le contenu de `table`. truncate=true : la table est vidée, pas recréée
    (types, clé primaire et valeur par défaut de computed_at conservés)."""
    df.write.option("truncate", "true").jdbc(JDBC_URL, table, mode="overwrite", properties=JDBC_PROPS)


# ---------------------------------------------------------------- main

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = SparkSession.builder.appName("mastodon-batch-job").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    start = time.perf_counter()

    toots = load_toots(spark)
    hashtags = load_hashtags(spark)

    users = active_users(toots, hashtags)
    overwrite(users, "batch_active_users")

    per_day = hashtag_daily(toots, hashtags)
    overwrite(per_day, "batch_hashtag_daily")

    totals = hashtag_totals(toots, hashtags)
    overwrite(totals, "batch_hashtag_totals")

    overwrite(daily_stats(toots, per_day), "batch_daily_stats")

    # Résumé lisible dans les logs (relit les tables écrites : peu de lignes)
    def read(table: str) -> DataFrame:
        return spark.read.jdbc(JDBC_URL, table, properties=JDBC_PROPS)

    n_toots = read("batch_daily_stats").agg(F.sum("toot_count")).first()[0] or 0
    active = read("batch_active_users").agg(F.count("*"), F.sum("toot_count")).first()
    log.info("%d toots analysés ; %d utilisateurs actifs (> %d toots) ont écrit %d toots (%.0f %%)",
             n_toots, active[0], ACTIVE_THRESHOLD, active[1] or 0,
             100 * (active[1] or 0) / max(n_toots, 1))
    top = read("batch_hashtag_totals").orderBy(F.desc("toot_count")).limit(10).collect()
    log.info("Top 10 hashtags : %s", ", ".join(f"#{r.hashtag} ({r.toot_count})" for r in top))
    log.info("Terminé en %.1f s", time.perf_counter() - start)
    spark.stop()


if __name__ == "__main__":
    main()

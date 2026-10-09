"""Job Spark Structured Streaming : Kafka (mastodon_stream) -> PostgreSQL.

Lit les toots JSON publiés par le producer, les nettoie, puis lance 5 requêtes de streaming :

    toots              couche brute : chaque toot (toutes langues) + ses hashtags,
                       source de vérité pour le batch (partie 3) et le ML (partie 4)
    counts_1min        Action 1 : nombre de toots par fenêtre d'1 minute (démo temps réel)
    counts_1hour       Action 1 : nombre de toots par fenêtre d'1 heure
    user_stats         Action 2 : nombre de toots et longueur moyenne par utilisateur et par heure
    hashtag_stats      Action 2 : nombre de toots et longueur moyenne par hashtag et par heure

Transformation 1 : filtre par langue (STREAM_LANGUAGES) et, optionnellement, par mots-clés
(STREAM_KEYWORDS), appliqué aux agrégats.
Transformation 2 : fenêtres temporelles sur created_at (heure de publication du toot), avec un
watermark (STREAM_WATERMARK) : les toots arrivant plus en retard sont ignorés par les agrégats
temps réel mais restent stockés dans `toots` et sont traités par le batch.

Écriture : foreachBatch -> table de staging (JDBC Spark) -> INSERT ... ON CONFLICT dans la table
finale. Les écritures sont donc idempotentes : un micro-batch rejoué après un crash, ou un toot
envoyé deux fois par le producer, ne crée pas de doublon.

Lancement (le service docker `stream-job` le fait automatiquement) :
    docker compose exec jupyter spark-submit /home/jovyan/work/streaming/stream_job.py
"""

import logging
import os
from contextlib import closing

import psycopg2
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

log = logging.getLogger("stream_job")

# ---------------------------------------------------------------- configuration

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "mastodon_stream")
STARTING_OFFSETS = os.environ.get("STREAM_STARTING_OFFSETS", "earliest")  # 1er lancement seulement
TRIGGER = os.environ.get("STREAM_TRIGGER", "10 seconds")
WATERMARK = os.environ.get("STREAM_WATERMARK", "1 hour")
LANGUAGES = [x.strip() for x in os.environ.get("STREAM_LANGUAGES", "en").split(",") if x.strip()]
KEYWORDS = [x.strip().lower() for x in os.environ.get("STREAM_KEYWORDS", "").split(",") if x.strip()]
CHECKPOINT_DIR = os.environ.get("STREAM_CHECKPOINT_DIR", "/home/jovyan/work/checkpoints/stream_job")

PG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_INTERNAL_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_DB", "mastodon"),
    "user": os.environ.get("POSTGRES_USER", "mastodon"),
    "password": os.environ.get("POSTGRES_PASSWORD", "mastodon"),
}
JDBC_URL = f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}"
JDBC_PROPS = {"user": PG["user"], "password": PG["password"], "driver": "org.postgresql.Driver"}

# Schéma des messages JSON du producer (cf. producer/toot_parser.py)
TOOT_SCHEMA = StructType([
    StructField("toot_id", StringType()),
    StructField("created_at", StringType()),
    StructField("user_id", StringType()),
    StructField("username", StringType()),
    StructField("language", StringType()),
    StructField("content", StringType()),
    StructField("hashtags", ArrayType(StringType())),
    StructField("favourites_count", IntegerType()),
    StructField("reblogs_count", IntegerType()),
    StructField("replies_count", IntegerType()),
    StructField("url", StringType()),
    StructField("location", StringType()),
    StructField("source_tag", StringType()),
    StructField("ingested_at", StringType()),
])


# ---------------------------------------------------------------- lecture et nettoyage

def read_toots(spark: SparkSession) -> DataFrame:
    """Kafka -> DataFrame structuré et nettoyé."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")  # tolère la purge du topic (rétention 7 jours)
        .load()
    )
    toots = (
        raw.select(F.from_json(F.col("value").cast("string"), TOOT_SCHEMA).alias("t"))
        .select("t.*")
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
    )
    # Texte nettoyé : sans URL ni mention, espaces normalisés. La longueur d'un toot est
    # mesurée sur ce texte, pour qu'un lien long ne fausse pas les statistiques.
    clean = F.regexp_replace("content", r"https?://\S+", "")
    clean = F.regexp_replace(clean, r"@[\w.@-]+", "")
    clean = F.trim(F.regexp_replace(clean, r"\s+", " "))
    return (
        toots.withColumn("content_clean", clean)
        .withColumn("content_length", F.length("content_clean"))
        .withColumn("hashtags", F.coalesce("hashtags", F.array().cast("array<string>")))
        .where(F.col("toot_id").isNotNull() & F.col("created_at").isNotNull())
        .where(F.col("content_clean") != "")
    )


def filter_for_analytics(toots: DataFrame) -> DataFrame:
    """Transformation 1 : langue (+ mots-clés optionnels), puis dédoublonnage avec watermark."""
    df = toots
    if LANGUAGES:
        df = df.where(F.col("language").isin(LANGUAGES))
    if KEYWORDS:
        pattern = "|".join(KEYWORDS)
        df = df.where(
            F.lower("content").rlike(pattern)
            | F.arrays_overlap("hashtags", F.array(*[F.lit(k) for k in KEYWORDS]))
        )
    # Le producer peut renvoyer un même toot (backfill à chaque redémarrage) : on ne le compte
    # qu'une fois. L'état de dédoublonnage est purgé au-delà du watermark.
    return df.withWatermark("created_at", WATERMARK).dropDuplicatesWithinWatermark(["toot_id"])


# ---------------------------------------------------------------- écriture PostgreSQL

def upsert(df: DataFrame, table: str, keys: list[str], update_cols: list[str] | None = None,
           touch_updated_at: bool = False, staging: str | None = None) -> int:
    """Écrit df dans `table` sans doublon : staging JDBC puis INSERT ... ON CONFLICT.

    update_cols=None -> les lignes déjà présentes sont ignorées (DO NOTHING).
    Chaque requête de streaming doit avoir sa propre table de staging (elles tournent en parallèle).
    """
    columns = df.columns
    staging = staging or f"stg_{table}"
    # truncate=true : la table de staging est vidée, pas recréée (types conservés).
    df.write.option("truncate", "true").jdbc(JDBC_URL, staging, mode="overwrite", properties=JDBC_PROPS)

    cols = ", ".join(columns)
    if update_cols:
        sets = [f"{c} = EXCLUDED.{c}" for c in update_cols]
        if touch_updated_at:
            sets.append("updated_at = now()")
        conflict = f"DO UPDATE SET {', '.join(sets)}"
    else:
        conflict = "DO NOTHING"
    sql = (f"INSERT INTO {table} ({cols}) SELECT {cols} FROM {staging} "
           f"ON CONFLICT ({', '.join(keys)}) {conflict}")
    with closing(psycopg2.connect(**PG)) as conn, conn, conn.cursor() as cur:  # commit puis fermeture
        cur.execute(sql)
        return cur.rowcount


def write_toots(batch: DataFrame, batch_id: int) -> None:
    """Couche brute : toots + relation toot/hashtag. Les compteurs d'engagement sont mis à jour
    si un toot déjà connu revient avec des valeurs plus récentes."""
    if batch.isEmpty():
        return
    batch = batch.dropDuplicates(["toot_id"]).persist()  # un même toot peut être 2 fois dans le batch
    n = upsert(
        batch.select("toot_id", "created_at", "user_id", "username", "language", "content",
                     "content_length", "favourites_count", "reblogs_count", "replies_count",
                     "source_tag", "ingested_at"),
        "toots", ["toot_id"],
        update_cols=["favourites_count", "reblogs_count", "replies_count"],
    )
    hashtags = batch.select("toot_id", F.explode("hashtags").alias("hashtag")).dropDuplicates()
    upsert(hashtags, "toot_hashtags", ["toot_id", "hashtag"])
    batch.unpersist()
    log.info("[toots] batch %d : %d toots insérés/mis à jour", batch_id, n)


def window_writer(name: str, table: str, keys: list[str], update_cols: list[str]):
    """foreachBatch pour un agrégat en mode update : chaque batch contient la valeur à jour
    des fenêtres modifiées, qu'on écrase dans PostgreSQL."""
    def write(batch: DataFrame, batch_id: int) -> None:
        if batch.isEmpty():
            return
        n = upsert(batch, table, keys, update_cols=update_cols, touch_updated_at=True,
                   staging=f"stg_{name}")
        log.info("[%s] batch %d : %d lignes", name, batch_id, n)
    return write


# ---------------------------------------------------------------- agrégations

def window_counts(df: DataFrame, duration: str) -> DataFrame:
    """Transformation 2 + Action 1 : nombre de toots par fenêtre."""
    return (
        df.groupBy(F.window("created_at", duration).alias("w"))
        .agg(F.count("*").alias("toot_count"))
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                F.lit(duration).alias("granularity"), "toot_count")
    )


def user_stats(df: DataFrame) -> DataFrame:
    """Action 2 : activité et longueur moyenne des toots par utilisateur, par heure."""
    return (
        df.groupBy(F.window("created_at", "1 hour").alias("w"), "user_id")
        .agg(F.max("username").alias("username"), F.count("*").alias("toot_count"),
             F.avg("content_length").alias("avg_length"))
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                "user_id", "username", "toot_count", "avg_length")
    )


def hashtag_stats(df: DataFrame) -> DataFrame:
    """Action 2 : nombre de toots et longueur moyenne par hashtag, par heure."""
    return (
        df.select("created_at", "content_length", F.explode("hashtags").alias("hashtag"))
        .groupBy(F.window("created_at", "1 hour").alias("w"), "hashtag")
        .agg(F.count("*").alias("toot_count"), F.avg("content_length").alias("avg_length"))
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                "hashtag", "toot_count", "avg_length")
    )


# ---------------------------------------------------------------- main

def start(df: DataFrame, name: str, writer, output_mode: str):
    return (
        df.writeStream.queryName(name)
        .outputMode(output_mode)
        .foreachBatch(writer)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{name}")
        .trigger(processingTime=TRIGGER)
        .start()
    )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = (
        SparkSession.builder.appName("mastodon-stream-job")
        # Petits volumes : 4 partitions de shuffle au lieu de 200. Ce nombre est figé dans les
        # checkpoints des agrégats : le changer impose de supprimer CHECKPOINT_DIR.
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("Kafka %s / topic %s -> %s | langues=%s mots-clés=%s watermark=%s trigger=%s",
             KAFKA_BOOTSTRAP, KAFKA_TOPIC, JDBC_URL, LANGUAGES or "toutes", KEYWORDS or "aucun",
             WATERMARK, TRIGGER)

    toots = read_toots(spark)
    analytics = filter_for_analytics(toots)

    queries = [
        start(toots, "toots", write_toots, "append"),
        start(window_counts(analytics, "1 minute"), "counts_1min",
              window_writer("counts_1min", "stream_window_counts", ["window_start", "granularity"],
                            ["window_end", "toot_count"]), "update"),
        start(window_counts(analytics, "1 hour"), "counts_1hour",
              window_writer("counts_1hour", "stream_window_counts", ["window_start", "granularity"],
                            ["window_end", "toot_count"]), "update"),
        start(user_stats(analytics), "user_stats",
              window_writer("user_stats", "stream_user_stats", ["window_start", "user_id"],
                            ["window_end", "username", "toot_count", "avg_length"]), "update"),
        start(hashtag_stats(analytics), "hashtag_stats",
              window_writer("hashtag_stats", "stream_hashtag_stats", ["window_start", "hashtag"],
                            ["window_end", "toot_count", "avg_length"]), "update"),
    ]

    # Suivi : toutes les minutes, nombre de lignes lues par requête lors du dernier micro-batch.
    while not spark.streams.awaitAnyTermination(60):
        status = {q.name: (q.lastProgress or {}).get("numInputRows", 0) for q in queries}
        log.info("Dernier micro-batch (lignes lues) : %s", status)
    for q in queries:
        if q.exception():
            raise q.exception()


if __name__ == "__main__":
    main()

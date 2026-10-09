"""Analyse de sentiment des toots historiques (partie 4).

Applique le modèle entraîné par ml/train_sentiment.py (models/sentiment/, nettoyage du texte compris)
aux toots en anglais de la table `toots`, et écrit le résultat dans `toot_sentiment` :

    positive   p(positif) > 0.5 + marge
    negative   p(positif) < 0.5 - marge
    neutral    entre les deux

La marge est celle choisie à l'entraînement (dernier modèle `selected` de sentiment_model_metrics).
Seuls les toots en anglais sont notés : le modèle a appris sur des tweets anglais. Un toot sans mot
connu du vocabulaire (uniquement des liens, des hashtags inconnus...) reçoit une probabilité proche
de 0.5 et tombe donc en neutre.

La table est recalculée entièrement à chaque exécution (vidée puis remplie) : relancer le script
après un nouvel entraînement ou une fois la collecte avancée.

Lancement :
    docker compose exec jupyter spark-submit /home/jovyan/work/ml/score_toots.py
"""

import logging
import os
import time

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("score_toots")

# ---------------------------------------------------------------- configuration

MODEL_DIR = os.environ.get("SENTIMENT_MODEL_DIR", "/home/jovyan/work/models/sentiment")
LANGUAGES = [x.strip() for x in os.environ.get("SENTIMENT_LANGUAGES", "en").split(",") if x.strip()]
JDBC_PARTITIONS = 4

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_INTERNAL_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "mastodon")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {
    "user": os.environ.get("POSTGRES_USER", "mastodon"),
    "password": os.environ.get("POSTGRES_PASSWORD", "mastodon"),
    "driver": "org.postgresql.Driver",
}


def read_sql(spark: SparkSession, query: str) -> DataFrame:
    return spark.read.jdbc(JDBC_URL, f"({query}) q", properties=JDBC_PROPS)


def selected_model(spark: SparkSession) -> tuple[str, float]:
    """Nom et marge neutre du dernier modèle sauvegardé par l'entraînement."""
    row = read_sql(spark, """
        SELECT model_name, neutral_margin FROM sentiment_model_metrics
        WHERE selected ORDER BY trained_at DESC LIMIT 1
    """).first()
    if row is None:
        raise SystemExit("Aucun modèle entraîné : lancer d'abord ml/train_sentiment.py")
    return row.model_name, row.neutral_margin


def load_toots(spark: SparkSession) -> DataFrame:
    """Toots à noter, lus en JDBC partitionné (hash du toot_id, cf. batch/batch_job.py)."""
    languages = ", ".join(f"'{lang}'" for lang in LANGUAGES if lang.isalpha())
    n = JDBC_PARTITIONS
    dbtable = f"""(SELECT toot_id, content, mod(abs(hashtext(toot_id)::bigint), {n}) AS part
                  FROM toots WHERE language IN ({languages})) t"""
    return (
        spark.read.jdbc(JDBC_URL, dbtable, column="part", lowerBound=0, upperBound=n,
                        numPartitions=n, properties={**JDBC_PROPS, "fetchsize": "5000"})
        .drop("part")
    )


def score(model: PipelineModel, toots: DataFrame, model_name: str, margin: float) -> DataFrame:
    p = F.col("probability")
    return (
        model.transform(toots.withColumn("text", F.col("content")))  # le pipeline attend `text`
        .withColumn("probability", vector_to_array("probability")[1])  # p(positif)
        .withColumn("sentiment",
                    F.when(F.abs(p - 0.5) <= margin, "neutral")
                    .when(p > 0.5, "positive")
                    .otherwise("negative"))
        .select("toot_id", "content", "sentiment", "probability", F.lit(model_name).alias("model_name"))
    )


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = SparkSession.builder.appName("mastodon-sentiment-scoring").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    start = time.perf_counter()

    model_name, margin = selected_model(spark)
    model = PipelineModel.load(MODEL_DIR)
    log.info("Modèle %s (%s), neutre si |p(positif) - 0.5| <= %.2f, langues %s",
             model_name, MODEL_DIR, margin, LANGUAGES)

    scored = score(model, load_toots(spark), model_name, margin)
    # truncate=true : la table est vidée, pas recréée (contraintes et clé étrangère conservées)
    scored.write.option("truncate", "true").jdbc(JDBC_URL, "toot_sentiment", mode="overwrite",
                                                 properties=JDBC_PROPS)

    counts = read_sql(spark, "SELECT sentiment, count(*) AS n FROM toot_sentiment GROUP BY sentiment").collect()
    total = sum(r.n for r in counts)
    log.info("%d toots notés : %s", total,
             ", ".join(f"{r.sentiment} {r.n} ({100 * r.n / total:.0f} %)" for r in sorted(counts)))
    log.info("Terminé en %.0f s", time.perf_counter() - start)
    spark.stop()


if __name__ == "__main__":
    main()

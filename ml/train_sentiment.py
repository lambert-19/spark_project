"""Entraînement du modèle de sentiment avec Spark MLlib (partie 4).

Données : Sentiment140 (1,6 M tweets de 2009, labels 0 = négatif / 4 = positif), à télécharger dans
data/sentiment140/ (cf. README) :
    training.1600000.processed.noemoticon.csv   entraînement (80 %) et test (20 %)
    testdata.manual.2009.06.14.csv              498 tweets annotés à la main, dont 139 neutres

Pipeline (sauvegardé en entier, nettoyage compris, pour être réappliqué tel quel aux toots) :
    SQLTransformer      nettoyage : minuscules, sans URL, mention, entité HTML, chiffre ni ponctuation
    RegexTokenizer      découpage en mots (2 caractères minimum)
    StopWordsRemover    mots vides anglais, SAUF les négations (not, no, don't...) : sans elles,
                        « not good » deviendrait « good »
    CountVectorizer     vocabulaire des VOCAB_SIZE mots les plus fréquents (présents dans >= 5 tweets)
    IDF                 pondération TF-IDF
    classifieur         LogisticRegression ou NaiveBayes (les deux sont entraînés et comparés)

Les étapes de préparation sont ajustées une seule fois et leur sortie mise en cache, puis les deux
classifieurs sont entraînés sur les mêmes vecteurs. Le modèle au meilleur AUC est sauvegardé dans
models/sentiment/ (préparation + classifieur dans un seul PipelineModel).

Neutre : Sentiment140 n'a que deux classes. Un toot est classé neutre si la probabilité « positif »
est dans [0.5 - marge, 0.5 + marge]. La marge est choisie sur le jeu annoté à la main (seule
source de tweets neutres) : celle qui maximise l'exactitude sur les 3 classes (la plus grande en
cas d'égalité). Ce jeu est petit (498 tweets) : un écart de 1 point = 5 tweets.

Métriques (exactitude, F1, AUC, matrice de confusion) : logs et table sentiment_model_metrics.

Lancement (une dizaine de minutes ; mémoire par défaut : 1 Go pour le driver et l'exécuteur) :
    docker compose exec jupyter spark-submit /home/jovyan/work/ml/train_sentiment.py
"""

import logging
import os
import time

from pyspark import StorageLevel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import (
    IDF,
    CountVectorizer,
    RegexTokenizer,
    SQLTransformer,
    StopWordsRemover,
)
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("train_sentiment")

# ---------------------------------------------------------------- configuration

WORK_DIR = "/home/jovyan/work"
DATA_DIR = os.environ.get("SENTIMENT_DATA_DIR", f"{WORK_DIR}/data/sentiment140")
MODEL_DIR = os.environ.get("SENTIMENT_MODEL_DIR", f"{WORK_DIR}/models/sentiment")
TRAIN_FRACTION = float(os.environ.get("SENTIMENT_TRAIN_FRACTION", "1.0"))  # < 1 pour un essai rapide
VOCAB_SIZE = int(os.environ.get("SENTIMENT_VOCAB_SIZE", "50000"))
# Régularisation de la régression logistique (cf. ml/tune_sentiment.py pour le réglage par validation croisée)
LR_REG_PARAM = float(os.environ.get("SENTIMENT_LR_REG_PARAM", "0.001"))
LR_ELASTIC_NET = float(os.environ.get("SENTIMENT_LR_ELASTIC_NET", "0.5"))  # 0 = L2, 1 = L1
SEED = 42
NEUTRAL_MARGINS = [0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3]

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_INTERNAL_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "mastodon")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {
    "user": os.environ.get("POSTGRES_USER", "mastodon"),
    "password": os.environ.get("POSTGRES_PASSWORD", "mastodon"),
    "driver": "org.postgresql.Driver",
}

SENTIMENT140_COLUMNS = "target INT, id LONG, date STRING, flag STRING, user STRING, text STRING"

# Nettoyage du texte (colonne `text` -> `clean`). Les antislashs sont doublés : la chaîne est un
# littéral SQL Spark, où '\\S' représente la classe de regex \S.
CLEAN_SQL = r"""
SELECT *, trim(regexp_replace(regexp_replace(regexp_replace(
           lower(translate(text, '’', "'")),
           'https?://\\S+|www\\.\\S+|@[\\w.@-]+|&\\w+;', ' '),
         "[^a-z' ]", ' '),
       '\\s+', ' ')) AS clean
FROM __THIS__
"""


# ---------------------------------------------------------------- données

def load_sentiment140(spark: SparkSession, file: str) -> DataFrame:
    """CSV Sentiment140 (encodé en latin-1, sans en-tête) -> text, target."""
    return (
        spark.read.csv(f"{DATA_DIR}/{file}", schema=SENTIMENT140_COLUMNS, encoding="ISO-8859-1")
        .select("text", "target")
        .where(F.col("text").isNotNull())
    )


# ---------------------------------------------------------------- modèle

def feature_pipeline() -> Pipeline:
    negations = {"no", "nor", "not", "never"}
    stop_words = [w for w in StopWordsRemover.loadDefaultStopWords("english")
                  if w not in negations and not w.endswith("n't")]
    return Pipeline(stages=[
        SQLTransformer(statement=CLEAN_SQL),
        RegexTokenizer(inputCol="clean", outputCol="tokens", pattern=r"\s+", minTokenLength=2),
        StopWordsRemover(inputCol="tokens", outputCol="words", stopWords=stop_words),
        CountVectorizer(inputCol="words", outputCol="tf", vocabSize=VOCAB_SIZE, minDF=5),
        IDF(inputCol="tf", outputCol="features"),
    ])


def evaluate(name: str, predictions: DataFrame) -> dict:
    """Exactitude, F1, AUC et matrice de confusion sur le jeu de test."""
    predictions = predictions.persist(StorageLevel.MEMORY_AND_DISK)
    multi = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    confusion = {(int(r.label), int(r.prediction)): r["count"]
                 for r in predictions.groupBy("label", "prediction").count().collect()}
    metrics = {
        "accuracy": multi.evaluate(predictions, {multi.metricName: "accuracy"}),
        "f1": multi.evaluate(predictions, {multi.metricName: "f1"}),
        # AUC calculée sur les probabilités : la colonne rawPrediction de NaiveBayes contient des
        # log-vraisemblances, non comparables d'un modèle à l'autre.
        "auc": BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="probability")
        .evaluate(predictions),
        "tn": confusion.get((0, 0), 0), "fp": confusion.get((0, 1), 0),
        "fn": confusion.get((1, 0), 0), "tp": confusion.get((1, 1), 0),
    }
    predictions.unpersist()
    log.info("%-18s exactitude=%.4f  F1=%.4f  AUC=%.4f  | VN=%d FP=%d FN=%d VP=%d", name,
             metrics["accuracy"], metrics["f1"], metrics["auc"],
             metrics["tn"], metrics["fp"], metrics["fn"], metrics["tp"])
    return metrics


def manual_test(model: PipelineModel, manual: DataFrame) -> dict:
    """Jeu annoté à la main : exactitude positif/négatif, puis choix de la marge « neutre »."""
    scored = (
        model.transform(manual)
        .select("target", vector_to_array("probability")[1].alias("p_positive"))
        .collect()
    )
    polar = [r for r in scored if r.target != 2]
    result = {"manual_accuracy": sum((r.p_positive >= 0.5) == (r.target == 4) for r in polar) / len(polar)}

    def label(p: float, margin: float) -> int:
        return 2 if abs(p - 0.5) <= margin else (4 if p > 0.5 else 0)

    by_margin = {m: sum(label(r.p_positive, m) == r.target for r in scored) / len(scored)
                 for m in NEUTRAL_MARGINS}
    for m, acc in by_margin.items():
        log.info("  marge neutre %.2f -> exactitude 3 classes %.4f", m, acc)
    # À exactitude égale, la plus grande marge : l'énoncé demande une classe neutre
    best = max(by_margin, key=lambda m: (by_margin[m], m))
    result.update(neutral_margin=best, manual_3class_accuracy=by_margin[best])
    log.info("Jeu manuel : exactitude pos/neg %.4f ; marge neutre retenue %.2f (exactitude 3 classes %.4f)",
             result["manual_accuracy"], best, by_margin[best])
    return result


# ---------------------------------------------------------------- main

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = (
        SparkSession.builder.appName("mastodon-sentiment-training")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    start = time.perf_counter()

    data = load_sentiment140(spark, "training.1600000.processed.noemoticon.csv")
    if TRAIN_FRACTION < 1:
        data = data.sample(fraction=TRAIN_FRACTION, seed=SEED)
    # 0/4 -> 0/1 ; en cache : le CSV n'est lu qu'une fois (ajustement du vocabulaire, de l'IDF...)
    data = data.withColumn("label", (F.col("target") / 4).cast("double")).persist(StorageLevel.MEMORY_AND_DISK)
    train, test = data.randomSplit([0.8, 0.2], seed=SEED)

    # Préparation ajustée une seule fois (vocabulaire, IDF) sur l'entraînement uniquement
    features = feature_pipeline().fit(train)
    train_f = features.transform(train).select("features", "label").persist(StorageLevel.MEMORY_AND_DISK)
    test_f = features.transform(test).select("features", "label").persist(StorageLevel.MEMORY_AND_DISK)
    n_train, n_test = train_f.count(), test_f.count()
    data.unpersist()  # les vecteurs sont en cache : le texte n'est plus utile (mémoire limitée)
    log.info("Entraînement %d tweets, test %d tweets, vocabulaire %d mots (%.0f s)",
             n_train, n_test, len(features.stages[3].vocabulary), time.perf_counter() - start)

    classifiers = {
        "logistic_regression": LogisticRegression(maxIter=30, regParam=LR_REG_PARAM,
                                                  elasticNetParam=LR_ELASTIC_NET),
        "naive_bayes": NaiveBayes(modelType="multinomial", smoothing=1.0),
    }
    results = {}
    for name, classifier in classifiers.items():
        t = time.perf_counter()
        fitted = classifier.fit(train_f)
        results[name] = {"model": fitted, **evaluate(name, fitted.transform(test_f))}
        results[name]["train_s"] = time.perf_counter() - t

    best = max(results, key=lambda n: results[n]["auc"])
    model = PipelineModel(stages=[*features.stages, results[best]["model"]])
    model.write().overwrite().save(MODEL_DIR)
    log.info("Modèle retenu : %s (meilleur AUC), sauvegardé dans %s", best, MODEL_DIR)

    manual = load_sentiment140(spark, "testdata.manual.2009.06.14.csv")
    manual_metrics = manual_test(model, manual)

    rows = [
        (name, name == best, MODEL_DIR if name == best else None, n_train, n_test,
         r["accuracy"], r["f1"], r["auc"], r["tn"], r["fp"], r["fn"], r["tp"], r["train_s"],
         *((manual_metrics["manual_accuracy"], manual_metrics["neutral_margin"],
            manual_metrics["manual_3class_accuracy"]) if name == best else (None, None, None)))
        for name, r in results.items()
    ]
    spark.createDataFrame(
        rows,
        "model_name string, selected boolean, model_path string, train_count long, test_count long, "
        "accuracy double, f1 double, auc double, tn long, fp long, fn long, tp long, train_s double, "
        "manual_accuracy double, neutral_margin double, manual_3class_accuracy double",
    ).write.jdbc(JDBC_URL, "sentiment_model_metrics", mode="append", properties=JDBC_PROPS)
    log.info("Terminé en %.0f s", time.perf_counter() - start)
    spark.stop()


if __name__ == "__main__":
    main()

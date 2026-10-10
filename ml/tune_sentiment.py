"""Bonus partie 4 : réglage des hyperparamètres de la régression logistique par validation croisée.

ml/train_sentiment.py utilise des paramètres fixés à la main (regParam=0.01, sans pénalité L1).
Ce script vérifie ce choix avec CrossValidator + ParamGridBuilder :

    grille      regParam ∈ {0.001, 0.01, 0.1} × elasticNetParam ∈ {0 (L2), 0.5 (L1+L2)}
    validation  3 plis, métrique AUC (comme pour le choix du modèle dans train_sentiment.py)
    données     échantillon de Sentiment140 (SENTIMENT_CV_FRACTION, défaut 10 % ≈ 160 000 tweets) :
                6 combinaisons × 3 plis = 18 entraînements, impossible sur 1,6 M de tweets avec
                2 cœurs et 1 Go par exécuteur

La préparation du texte (nettoyage, tokens, TF-IDF) est celle de train_sentiment.py ; elle est
ajustée une seule fois, hors validation croisée, pour ne pas la recalculer 18 fois. Le vocabulaire
voit donc aussi les plis de validation : léger biais optimiste, identique pour toutes les
combinaisons, sans effet sur leur classement.

Le meilleur modèle est ensuite évalué sur un jeu de test tenu à l'écart (20 % de l'échantillon).
Résultats : logs et table sentiment_cv_results. Le modèle sauvegardé n'est pas modifié.

Lancement (~5 min ; mettre le streaming en pause si Docker a 8 Go ou moins, cf. README) :
    docker compose exec jupyter spark-submit /home/jovyan/work/ml/tune_sentiment.py
"""

import logging
import os
import time

from pyspark import StorageLevel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from train_sentiment import (
    JDBC_PROPS,
    JDBC_URL,
    SEED,
    evaluate,
    feature_pipeline,
    load_sentiment140,
)

log = logging.getLogger("tune_sentiment")

CV_FRACTION = float(os.environ.get("SENTIMENT_CV_FRACTION", "0.1"))
FOLDS = 3


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = (
        SparkSession.builder.appName("mastodon-sentiment-tuning")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    start = time.perf_counter()

    data = (
        load_sentiment140(spark, "training.1600000.processed.noemoticon.csv")
        .sample(fraction=CV_FRACTION, seed=SEED)
        .withColumn("label", (F.col("target") / 4).cast("double"))
    )
    train, test = data.randomSplit([0.8, 0.2], seed=SEED)
    features = feature_pipeline().fit(train)
    train_f = features.transform(train).select("features", "label").persist(StorageLevel.MEMORY_AND_DISK)
    test_f = features.transform(test).select("features", "label").persist(StorageLevel.MEMORY_AND_DISK)
    log.info("Échantillon %.0f %% : entraînement %d tweets, test %d tweets",
             CV_FRACTION * 100, train_f.count(), test_f.count())

    lr = LogisticRegression(maxIter=30)
    grid = (
        ParamGridBuilder()
        .addGrid(lr.regParam, [0.001, 0.01, 0.1])
        .addGrid(lr.elasticNetParam, [0.0, 0.5])
        .build()
    )
    auc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="probability")
    cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=auc,
                        numFolds=FOLDS, seed=SEED, parallelism=2)
    t = time.perf_counter()
    cv_model = cv.fit(train_f)
    cv_s = time.perf_counter() - t

    rows = []
    best_index = max(range(len(grid)), key=lambda i: cv_model.avgMetrics[i])
    for i, (params, mean_auc, std_auc) in enumerate(zip(grid, cv_model.avgMetrics, cv_model.stdMetrics)):
        reg, enet = params[lr.regParam], params[lr.elasticNetParam]
        log.info("regParam=%-6g elasticNetParam=%-4g AUC moyenne %.4f (± %.4f)%s",
                 reg, enet, mean_auc, std_auc, "  <- meilleur" if i == best_index else "")
        rows.append((float(reg), float(enet), FOLDS, float(mean_auc), float(std_auc), i == best_index))

    best = cv_model.bestModel
    test_metrics = evaluate("meilleur (test)", best.transform(test_f))
    log.info("Validation croisée : %d entraînements en %.0f s ; meilleur : regParam=%g, elasticNetParam=%g",
             len(grid) * FOLDS, cv_s, best.getRegParam(), best.getElasticNetParam())

    spark.createDataFrame(
        [(*r, CV_FRACTION, *((test_metrics["accuracy"], test_metrics["auc"]) if r[-1] else (None, None)))
         for r in rows],
        "reg_param double, elastic_net_param double, folds int, cv_auc_mean double, cv_auc_std double, "
        "best boolean, sample_fraction double, test_accuracy double, test_auc double",
    ).write.jdbc(JDBC_URL, "sentiment_cv_results", mode="append", properties=JDBC_PROPS)
    log.info("Terminé en %.0f s", time.perf_counter() - start)
    spark.stop()


if __name__ == "__main__":
    main()

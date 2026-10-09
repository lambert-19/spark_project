# Image Spark commune à tout le cluster : master, worker, history server et Jupyter.
# Utiliser la même image partout garantit que le driver (Jupyter) et les executors
# (worker) ont exactement les mêmes versions de Spark, Java et Python.
FROM quay.io/jupyter/pyspark-notebook:spark-4.1.2

ARG SCALA_VERSION=2.13
ARG SPARK_VERSION=4.1.2
ARG KAFKA_CLIENTS_VERSION=3.9.1
ARG COMMONS_POOL2_VERSION=2.12.1
ARG POSTGRES_JDBC_VERSION=42.7.14
ARG MAVEN=https://repo1.maven.org/maven2

USER root

# Connecteur Kafka pour Structured Streaming + ses dépendances d'exécution,
# et driver JDBC PostgreSQL. Ils sont intégrés à l'image pour éviter un
# téléchargement Ivy à chaque démarrage (et pour que la démo fonctionne hors ligne).
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar ${SPARK_HOME}/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar ${SPARK_HOME}/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar ${SPARK_HOME}/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar ${SPARK_HOME}/jars/
ADD --chmod=644 ${MAVEN}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar ${SPARK_HOME}/jars/

COPY --chmod=644 spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

USER ${NB_UID}

# Accès direct à PostgreSQL depuis les notebooks (pandas.read_sql pour les graphiques)
RUN pip install --no-cache-dir "psycopg2-binary>=2.9"

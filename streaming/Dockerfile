FROM bitnamilegacy/spark:3.5.0

USER root

# Instalar dependências Python
COPY requirements.txt /opt/bitnami/spark/requirements.txt
RUN pip install --no-cache-dir -r /opt/bitnami/spark/requirements.txt

# Copiar scripts
COPY . /opt/bitnami/spark/app
WORKDIR /opt/bitnami/spark/app

# O comando padrão pode ser sobrescrito pelo docker-compose
CMD ["spark-submit", "--packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.2", "bronze_ingestion.py"]

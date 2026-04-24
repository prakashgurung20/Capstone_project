FROM apache/spark:3.5.1 AS spark
USER root
WORKDIR /app

RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

COPY . .

# Download Kafka connector JAR directly into Spark's jars folder
RUN curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    -o /opt/spark/jars/kafka-clients-3.4.1.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -o /opt/spark/jars/commons-pool2-2.11.1.jar

RUN pip3 install kafka-python==2.0.2 pyarrow==16.1.0 python-dotenv==1.0.1 pyyaml==6.0.1

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN mkdir -p /data/bronze/payments /data/checkpoints

CMD ["python3", "spark/jobs/bronze_streaming.py", \
     "--kafka-bootstrap", "kafka:29092", \
     "--bronze-path", "/data/bronze/payments", \
     "--checkpoint-path", "/data/checkpoints"]
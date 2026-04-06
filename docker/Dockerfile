# ── Producer stage ────────────────────────────────────────────
FROM python:3.11-slim AS producer
WORKDIR /app
COPY . .
RUN pip install kafka-python==2.0.2 python-dotenv==1.0.1 pyyaml==6.0.1 \
    --break-system-packages

# ── Spark stage ───────────────────────────────────────────────
FROM apache/spark:3.5.1 AS spark
USER root
WORKDIR /app

RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

COPY . .
RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install kafka-python==2.0.2 pyarrow==16.1.0 python-dotenv==1.0.1 pyyaml==6.0.1

RUN mkdir -p /data/bronze/payments /data/checkpoints

CMD ["python3", "spark/jobs/bronze_streaming.py"]
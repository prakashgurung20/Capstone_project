import argparse
import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("PaymentsETL")


BLACKLISTED_MERCHANTS = {"M11111", "M22222"}
HIGH_AMOUNT_THRESHOLD = 10_000.00
VELOCITY_MAX_TXN      = 5
VALID_CURRENCIES      = ["USD", "EUR", "GBP", "NPR"]

PAYMENT_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("ts_event",       T.StringType()),
    T.StructField("card_hash",      T.StringType()),
    T.StructField("merchant_id",    T.StringType()),
    T.StructField("amount",         T.DoubleType()),
    T.StructField("currency",       T.StringType()),
    T.StructField("mcc",            T.StringType()),
    T.StructField("channel",        T.StringType()),
    T.StructField("auth_result",    T.StringType()),
    T.StructField("location",       T.StringType()),
])

# ─────────────────────────────────────────────────────────────
# Spark
# ─────────────────────────────────────────────────────────────
def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

# ─────────────────────────────────────────────────────────────
# Source
# ─────────────────────────────────────────────────────────────
def read_payments_raw(spark: SparkSession, kafka_bootstrap: str) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "payments.raw")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) as json")
        .select(F.from_json(F.col("json"), PAYMENT_SCHEMA).alias("data"))
        .select("data.*")
    )

# ─────────────────────────────────────────────────────────────
# Helpers (DRY)
# ─────────────────────────────────────────────────────────────
def parse_timestamp(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "event_time",
        F.to_timestamp("ts_event", "yyyy-MM-dd HH:mm:ss"),
    )

def add_partition_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("year", F.year("event_time"))
        .withColumn("month", F.month("event_time"))
        .withColumn("day", F.dayofmonth("event_time"))
    )

def extract_country(df: DataFrame) -> DataFrame:
    parts = F.split(F.col("location"), ",")
    return df.withColumn(
        "country",
        F.when(F.size(parts) > 1, F.trim(F.element_at(parts, -1)))
         .otherwise(F.lit("UNKNOWN")),
    )

# ─────────────────────────────────────────────────────────────
# Fraud Logic
# ─────────────────────────────────────────────────────────────
def apply_fraud_rules(df: DataFrame) -> DataFrame:
    df = extract_country(df)

    # scalar rules
    df = df.withColumn("is_high_amount", F.col("amount") > HIGH_AMOUNT_THRESHOLD)
    df = df.withColumn("is_blacklisted", F.col("merchant_id").isin(BLACKLISTED_MERCHANTS))
    df = df.withColumn("is_bank_declined", F.col("auth_result") == "DECLINED")

    # velocity rule
    velocity = (
        df.groupBy(F.window("event_time", "1 minute"), "card_hash")
        .count()
        .filter(F.col("count") > VELOCITY_MAX_TXN)
        .select(
            F.col("card_hash").alias("v_card"),
            F.col("window.start").alias("v_start"),
            F.col("window.end").alias("v_end"),
        )
    )

    df = (
        df.join(
            velocity,
            (df.card_hash == velocity.v_card) &
            (df.event_time >= velocity.v_start) &
            (df.event_time < velocity.v_end),
            "left"
        )
        .withColumn("is_high_velocity", F.col("v_card").isNotNull())
        .drop("v_card", "v_start", "v_end")
    )

    # cross-border rule
    cb = (
        df.groupBy(F.window("event_time", "10 minutes"), "card_hash")
        .agg(F.countDistinct("country").alias("country_count"))
        .filter(F.col("country_count") > 1)
        .select(
            F.col("card_hash").alias("cb_card"),
            F.col("window.start").alias("cb_start"),
            F.col("window.end").alias("cb_end"),
        )
    )

    df = (
        df.join(
            cb,
            (df.card_hash == cb.cb_card) &
            (df.event_time >= cb.cb_start) &
            (df.event_time < cb.cb_end),
            "left"
        )
        .withColumn("is_cross_border", F.col("cb_card").isNotNull())
        .drop("cb_card", "cb_start", "cb_end")
    )

    # decline rate rule
    dr = (
        df.groupBy(F.window("event_time", "10 minutes"), "card_hash")
        .agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("auth_result") == "DECLINED", 1).otherwise(0)).alias("declined")
        )
        .withColumn("rate", F.col("declined") / F.col("total"))
        .filter(F.col("rate") > 0.5)
        .select(
            F.col("card_hash").alias("dr_card"),
            F.col("window.start").alias("dr_start"),
            F.col("window.end").alias("dr_end"),
        )
    )

    df = (
        df.join(
            dr,
            (df.card_hash == dr.dr_card) &
            (df.event_time >= dr.dr_start) &
            (df.event_time < dr.dr_end),
            "left"
        )
        .withColumn("is_high_decline_rate", F.col("dr_card").isNotNull())
        .drop("dr_card", "dr_start", "dr_end")
    )

    # fraud reasons
    df = df.withColumn(
        "fraud_reasons",
        F.concat_ws(
            ", ",
            F.when(F.col("is_high_amount"), F.lit("HIGH_AMOUNT")),
            F.when(F.col("is_blacklisted"), F.lit("BLACKLISTED_MERCHANT")),
            F.when(F.col("is_bank_declined"), F.lit("BANK_DECLINED")),
            F.when(F.col("is_high_velocity"), F.lit("VELOCITY")),
            F.when(F.col("is_cross_border"), F.lit("CROSS_BORDER")),
            F.when(F.col("is_high_decline_rate"), F.lit("HIGH_DECLINE_RATE")),
        )
    )

    # master fraud flag
    df = df.withColumn(
        "is_fraud",
        F.col("is_high_amount") |
        F.col("is_blacklisted") |
        F.col("is_bank_declined") |
        F.col("is_high_velocity") |
        F.col("is_cross_border") |
        F.col("is_high_decline_rate")
    )

    return df.drop("country")


def process_batch(batch_df: DataFrame, batch_id: int, bronze_path: str):
    count = batch_df.count()
    log.info(f"[Batch {batch_id}] received {count} rows")

    if count == 0:
        return None, None

    # 1. parse timestamp
    df = parse_timestamp(batch_df)

    # 2. dead-letter bad timestamps
    bad = df.filter(F.col("event_time").isNull())
    if bad.count() > 0:
        dl_path = f"{os.path.dirname(bronze_path)}/dead_letter/batch_{batch_id}"
        bad.write.mode("append").json(dl_path)
        log.warning(f"Dead-lettered bad rows → {dl_path}")

    # 3. clean
    df = df.filter(F.col("event_time").isNotNull())
    if df.count() == 0:
        return None, None

    # 4. enrich
    df = add_partition_columns(df)

    df = df.withColumn(
        "is_invalid",
        ~F.col("currency").isin(VALID_CURRENCIES)
    )

    df = apply_fraud_rules(df)

    # 5. split
    valid = df.filter(~F.col("is_fraud") & ~F.col("is_invalid"))
    fraud = df.filter(F.col("is_fraud") | F.col("is_invalid"))

    log.info(f"[Batch {batch_id}] valid={valid.count()} fraud={fraud.count()}")

    return valid, fraud


def write_to_bronze(raw_df: DataFrame, bronze_path: str, checkpoint: str):
    os.makedirs(bronze_path, exist_ok=True)

    def foreach_batch(df, batch_id):
        valid, _ = process_batch(df, batch_id, bronze_path)

        if valid is None or valid.count() == 0:
            return

        (
            valid
            .drop("is_fraud", "is_invalid", "fraud_reasons")
            .write
            .partitionBy("year", "month", "day")
            .mode("append")
            .parquet(bronze_path)
        )

    return (
        raw_df.writeStream
        .foreachBatch(foreach_batch)
        .option("checkpointLocation", f"{checkpoint}/bronze")
        .trigger(processingTime="10 seconds")
        .start()
    )

def write_to_deadletter(raw_df: DataFrame, kafka_bootstrap: str, checkpoint: str, bronze_path: str):
    def foreach_batch(df, batch_id):
        _, fraud = process_batch(df, batch_id, bronze_path)

        if fraud is None or fraud.count() == 0:
            return

        (
            fraud
            .selectExpr("card_hash as key", "to_json(struct(*)) as value")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap)
            .option("topic", "payments.deadletter")
            .save()
        )

    return (
        raw_df.writeStream
        .foreachBatch(foreach_batch)
        .option("checkpointLocation", f"{checkpoint}/deadletter")
        .trigger(processingTime="20 seconds")
        .start()
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-bootstrap", default="kafka:29092")
    parser.add_argument("--bronze-path", default="/data/bronze/payments")
    parser.add_argument("--checkpoint-path", default="/data/checkpoints")
    args = parser.parse_args()

    spark = build_spark("PaymentsStreamingETL")

    raw_df = read_payments_raw(spark, args.kafka_bootstrap)

    q1 = write_to_bronze(raw_df, args.bronze_path, args.checkpoint_path)
    q2 = write_to_deadletter(raw_df, args.kafka_bootstrap, args.checkpoint_path, args.bronze_path)

    spark.streams.awaitAnyTermination()
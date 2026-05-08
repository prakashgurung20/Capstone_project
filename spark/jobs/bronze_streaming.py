import argparse
import logging
import os
import shutil

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("PaymentsETL")

# Constants
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


def build_spark(app_name: str) -> SparkSession:
    
    

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC") 
        .getOrCreate()
    )


def parse_timestamp(df: DataFrame) -> DataFrame:
    return df.withColumn("event_time", F.to_timestamp("ts_event", "yyyy-MM-dd HH:mm:ss"))

def add_partition_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("year", F.year("event_time"))
        .withColumn("month", F.month("event_time"))
        .withColumn("day", F.dayofmonth("event_time"))
    )

def extract_country(df: DataFrame) -> DataFrame:
    parts = F.split(F.col("location"), ",")
    return df.withColumn(
        "country",
        F.when(F.size(parts) > 1, F.trim(F.element_at(parts, -1))).otherwise(F.lit("UNKNOWN"))
    )

def apply_fraud_rules(df: DataFrame) -> DataFrame:
    df = extract_country(df)

    # Simple Flags
    df = df.withColumn("is_high_amount", F.col("amount") > HIGH_AMOUNT_THRESHOLD)
    df = df.withColumn("is_blacklisted", F.col("merchant_id").isin(BLACKLISTED_MERCHANTS))
    df = df.withColumn("is_bank_declined", F.col("auth_result") == "DECLINED")

    # Velocity Join (Triggers Shuffle)
    velocity = (
        df.groupBy(F.window("event_time", "1 minute"), "card_hash")
        .count()
        .filter(F.col("count") > VELOCITY_MAX_TXN)
        .select(F.col("card_hash").alias("v_card"), F.col("window.start").alias("v_start"), F.col("window.end").alias("v_end"))
    )

    df = df.join(velocity, (df.card_hash == velocity.v_card) & (df.event_time >= velocity.v_start) & (df.event_time < velocity.v_end), "left") \
           .withColumn("is_high_velocity", F.col("v_card").isNotNull()).drop("v_card", "v_start", "v_end")

   
    cb = (
        df.groupBy(F.window("event_time", "10 minutes"), "card_hash")
        .agg(F.countDistinct("country").alias("country_count"))
        .filter(F.col("country_count") > 1)
        .select(F.col("card_hash").alias("cb_card"), F.col("window.start").alias("cb_start"), F.col("window.end").alias("cb_end"))
    )

    df = df.join(cb, (df.card_hash == cb.cb_card) & (df.event_time >= cb.cb_start) & (df.event_time < cb.cb_end), "left") \
           .withColumn("is_cross_border", F.col("cb_card").isNotNull()).drop("cb_card", "cb_start", "cb_end")

    
    df = df.withColumn("is_fraud", F.col("is_high_amount") | F.col("is_blacklisted") | F.col("is_bank_declined") | F.col("is_high_velocity") | F.col("is_cross_border"))
    
    return df


def main_pipeline(kafka_bootstrap, bronze_path, checkpoint_path):
    spark = build_spark("PaymentsStreamingETL")

    # Read Source
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "payments.raw")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) as json")
        .select(F.from_json(F.col("json"), PAYMENT_SCHEMA).alias("data"))
        .select("data.*")
    )

    def consolidated_foreach(batch_df, batch_id):
        if  batch_df.isEmpty():
            return

        log.info(f"--- Processing Batch {batch_id} ---")
        
        
        df = parse_timestamp(batch_df)
        df = df.filter(F.col("event_time").isNotNull())
        df = add_partition_columns(df)
        df = df.withColumn("is_invalid", ~F.col("currency").isin(VALID_CURRENCIES))
        df = apply_fraud_rules(df)

        
        valid_df = df.filter(~F.col("is_fraud") & ~F.col("is_invalid"))
        fraud_df = df.filter(F.col("is_fraud") | F.col("is_invalid"))

        
        if not valid_df.isEmpty():
            (valid_df.drop("is_fraud", "is_invalid")
             .write.mode("append")
             .partitionBy("year", "month", "day")
             .parquet(bronze_path))
            log.info(f"Batch {batch_id}: Saved {valid_df.count()} valid records to Bronze.")

        # 4. Write Dead Letter (Kafka)
        if not fraud_df.isEmpty():
            (fraud_df.selectExpr("card_hash as key", "to_json(struct(*)) as value")
             .write.format("kafka")
             .option("kafka.bootstrap.servers", kafka_bootstrap)
             .option("topic", "payments.deadletter")
             .save())
            log.info(f"Batch {batch_id}: Sent {fraud_df.count()} flagged records to DeadLetter.")

    
    query = (
        raw_stream.writeStream
        .foreachBatch(consolidated_foreach)
        .option("checkpointLocation", f"{checkpoint_path}/unified_pipeline")
        .trigger(processingTime="15 seconds")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-bootstrap", default="localhost:9092")
    parser.add_argument("--bronze-path", default="C:/data/bronze/payments")
    parser.add_argument("--checkpoint-path", default="C:/data/checkpoints")
    args = parser.parse_args()

    main_pipeline(args.kafka_bootstrap, args.bronze_path, args.checkpoint_path)
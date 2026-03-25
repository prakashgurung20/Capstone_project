import argparse
import json 
from pyspark.sql import SparkSession, functions as F, types as  T
from pyspark.sql.window import Window
from pyspark.sql.types import *

BLACKLISTED_MERCHANTS = {"M11111", "M22222"}
HIGH_AMOUNT_THRESHOLD = 10_000.00
VELOCITY_WINDOW_SECS  = 60          
VELOCITY_MAX_TXN      = 5

def build_spark(app_name: str) -> SparkSession:
    spark =(
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions","8")
        .getOrCreate()
    )
    return spark

def read_payments_raw(spark: SparkSession, kafka_bootstrap:str):


    schema = T.StructType([
        T.StructField("transaction_id", T.StringType()),
        T.StructField("ts_event", T.StringType()),
        T.StructField("card_hash", T.StringType()),
        T.StructField("merchant_id", T.StringType()),
        T.StructField("amount", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("mcc", T.StringType()),
        T.StructField("channel", T.StringType()),
        T.StructField("auth_result", T.StringType()),
        T.StructField("location", T.StringType()),
    ])

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",kafka_bootstrap)
        .option("subscribe","payments_raw")
        .option("startingOffsets", "latest")
        .load()
    )
    return df

    
def parse_stream(df_raw, schema):
    df_json = df_raw.selectExpr("CAST(value AS STRING) as json")

    df = (
        df_json
        .select(F.from_json(F.col("json"), schema).alias("data"))
        .select("data.*")
    )

    return df

def validate(df):
    df.withColumn(
        "is_invalid",
        (F.col("transaction_id").isNotNull()) &
        (F.col("amount") < 0) &
        (F.col("currency").isin(["USD", "EUR", "GBP", "NPR"]))
    )
    return df
#applied frad rules
def apply_fraud_rules(df, amount_threshold, BLACKLISTED_MERCHANTS):

    df = df.withColumn("event_time", F.to_timestamp("ts_event")) \
           .withWatermark("event_time", "2 minutes")

    df = df.withColumn(
        "fraud_reason",
        F.concat_ws(",",
            F.when(F.col("amount") > amount_threshold, "HIGH_AMOUNT"),
            F.when(F.col("merchant_id").isin(BLACKLISTED_MERCHANTS), "BLACKLISTED"),
            F.when(F.col("auth_result") == "DECLINED", "BANK_DECLINED")
        )
    )


    window_spec = Window.partitionBy("card_hash") \
    .orderBy(F.col("event_time").cast("long")) \
    .rangeBetween(-VELOCITY_WINDOW_SECS, 0)
    df = df.withColumn("txn_count", F.count("*").over(window_spec))
    
    df = df.withColumn(
        "is_fraud",
        (F.col("fraud_reason").isNotNull()) | (F.col("txn_count") > 5)
    )
    
    return df

def write_to_bronze(df, path, checkpoint):
    df =(
        df.filter((F.col("is_fraud") == False) & (F.col("is_invalid") == False))
        .withColumn("event_date", F.to_date("event_time"))
        .writeStream
        .format("parquet")
        .option("path", path)
        .option("checkpointLocation", f"{checkpoint}/bronze")
        .partitionBy("event_date")
        .start()
    )
    return df

def write_to_deadletter(df,kafka_bootstrap,checkpoint):
    df=(
        df.filter((F.col("is_fraud") == True) | (F.col("is_invalid") == True))
        .select(F.to_json(F.struct("*")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", "payments.deadletter")
        .option("checkpointLocation", f"{checkpoint}/deadletter")
        .start()

    )
    return df





   


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--kafka-bootstrap", default="localhost:9092")
    p.add_argument("--bronze-path", default="./data/bronze/payments")
    p.add_argument("--checkpoint-path", default="./data/checkpoints")

    args = p.parse_args()

    spark = build_spark("PaymentsStreamingETL")

    schema = T.StructType([
        T.StructField("transaction_id", T.StringType()),
        T.StructField("ts_event", T.StringType()),
        T.StructField("card_hash", T.StringType()),
        T.StructField("merchant_id", T.StringType()),
        T.StructField("amount", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("mcc", T.StringType()),
        T.StructField("channel", T.StringType()),
        T.StructField("auth_result", T.StringType()),
        T.StructField("location", T.StringType()),
    ])

    raw_df = read_payments_raw(spark, args.kafka_bootstrap)

    parsed_df = parse_stream(raw_df, schema)

    validated_df = validate(parsed_df)

    enriched_df = apply_fraud_rules(validated_df, HIGH_AMOUNT_THRESHOLD, BLACKLISTED_MERCHANTS)

    q1 = write_to_bronze(enriched_df, args.bronze_path, args.checkpoint_path)

    q2 = write_to_deadletter(enriched_df, args.kafka_bootstrap, args.checkpoint_path)

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()






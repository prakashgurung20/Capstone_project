import argparse
import json 
from pyspark.sql import SparkSession, functions as F, types as  T


BLACKLISTED_MERCHANTS = {"M11111", "M22222"}


def build_spark(app_name: str) -> SparkSession:
    spark =(
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions","8")
        .getOrCreate()
    )
    return spark

def read_payments_raw(spark: SparkSession, path:str):


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
        .schema(schema)
        .option("kafka.bootstrap.servers","localhost:9092")
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
    return df.filter(
        (F.col("transaction_id").isNotNull()) &
        (F.col("amount") >= 0) &
        (F.col("currency").isin(["USD", "EUR", "GBP", "NPR"]))
    )
#applied frad rules
def apply_fraud_rules(df, amount_threshold, BLACKLISTED_MERCHANTS):

    df = df.withColumn("event_time", F.to_timestamp("ts_event"))

    df = df.withColumn(
        "fraud_reason",
        F.concat_ws(",",
            F.when(F.col("amount") > amount_threshold, "HIGH_AMOUNT"),
            F.when(F.col("merchant_id").isin(BLACKLISTED_MERCHANTS), "BLACKLISTED"),
            F.when(F.col("auth_result") == "DECLINED", "DECLINED")
        )
    )

    df = df.withColumn(
        "is_fraud",
        F.col("fraud_reason") != ""
    )
    return df

   


# def main():
    







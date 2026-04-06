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



def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")                              
        .config("spark.sql.shuffle.partitions", "4")     
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"  
        )
        .getOrCreate()
    )


def read_payments_raw(spark: SparkSession, kafka_bootstrap: str):
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



def validate_and_flag(batch_df: DataFrame,
                      amount_threshold: float,
                      blacklisted: set) -> DataFrame:

   
    df = batch_df.withColumn(
        "event_time",
        F.to_timestamp("ts_event", "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )

    
    df = (
        df
        .withColumn("year",  F.year("event_time"))
        .withColumn("month", F.month("event_time"))
        .withColumn("date",  F.dayofmonth("event_time"))
    )

   
    df = df.withColumn(
        "is_invalid",
        ~F.col("currency").isin(VALID_CURRENCIES),
    )

   
    parts = F.split(F.col("location"), ",")
    df = df.withColumn(
        "country",
        F.when(F.size(parts) > 1, F.trim(F.element_at(parts, -1)))
         .otherwise(F.lit("UNKNOWN")),
    )

    
    df = df.withColumn("is_high_amount", F.col("amount") > amount_threshold)

    
    df = df.withColumn("is_blacklisted", F.col("merchant_id").isin(blacklisted))

  
    df = df.withColumn("is_bank_declined", F.col("auth_result") == "DECLINED")

    
    velocity_agg = (
        df.groupBy(F.window("event_time", "1 minute"), "card_hash")
        .agg(F.count("*").alias("velocity_count"))
        .filter(F.col("velocity_count") > VELOCITY_MAX_TXN)
        .select(
            F.col("card_hash").alias("v_card"),
            F.col("window.start").alias("v_start"),
            F.col("window.end").alias("v_end"),
        )
    )
    df = (
        df.join(
            velocity_agg,
            (df.card_hash  == velocity_agg.v_card) &
            (df.event_time >= velocity_agg.v_start) &
            (df.event_time <  velocity_agg.v_end),
            how="left",
        )
        .withColumn("is_high_velocity", F.col("v_card").isNotNull())
        .drop("v_card", "v_start", "v_end")
    )

    
    cb_agg = (
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
            cb_agg,
            (df.card_hash  == cb_agg.cb_card) &
            (df.event_time >= cb_agg.cb_start) &
            (df.event_time <  cb_agg.cb_end),
            how="left",
        )
        .withColumn("is_cross_border", F.col("cb_card").isNotNull())
        .drop("cb_card", "cb_start", "cb_end")
    )

    
    dr_agg = (
        df.groupBy(F.window("event_time", "10 minutes"), "card_hash")
        .agg(
            F.count("*").alias("total_txn"),
            F.sum(F.when(F.col("auth_result") == "DECLINED", 1).otherwise(0))
             .alias("declined_txn"),
        )
        .withColumn("decline_rate", F.col("declined_txn") / F.col("total_txn"))
        .filter(F.col("decline_rate") > 0.5)
        .select(
            F.col("card_hash").alias("dr_card"),
            F.col("window.start").alias("dr_start"),
            F.col("window.end").alias("dr_end"),
        )
    )
    df = (
        df.join(
            dr_agg,
            (df.card_hash  == dr_agg.dr_card) &
            (df.event_time >= dr_agg.dr_start) &
            (df.event_time <  dr_agg.dr_end),
            how="left",
        )
        .withColumn("is_high_decline_rate", F.col("dr_card").isNotNull())
        .drop("dr_card", "dr_start", "dr_end")
    )

    
    df = df.withColumn(
        "fraud_reasons",
        F.concat_ws(
            ", ",
            F.when(F.col("is_high_amount"),       F.lit("HIGH_AMOUNT")),
            F.when(F.col("is_blacklisted"),        F.lit("BLACKLISTED_MERCHANT")),
            F.when(F.col("is_bank_declined"),      F.lit("BANK_DECLINED")),
            F.when(F.col("is_high_velocity"),      F.lit("VELOCITY")),
            F.when(F.col("is_cross_border"),       F.lit("CROSS_BORDER")),
            F.when(F.col("is_high_decline_rate"),  F.lit("HIGH_DECLINE_RATE")),
        ),
    )

    df = df.withColumn(
        "is_fraud",
        F.col("is_high_amount")      |
        F.col("is_blacklisted")      |
        F.col("is_bank_declined")    |
        F.col("is_high_velocity")    |
        F.col("is_cross_border")     |
        F.col("is_high_decline_rate"),
    )

    return df.drop(
        "country",
        "is_high_amount", "is_blacklisted", "is_bank_declined",
        "is_high_velocity", "is_cross_border", "is_high_decline_rate",
    )



def write_to_bronze(raw_df: DataFrame, bronze_path: str, checkpoint: str,
                    amount_threshold: float, blacklisted: set):

   
    os.makedirs(bronze_path, exist_ok=True)

    print(f"Bronze output path: {bronze_path}")
    print(f"Path exists: {os.path.exists(bronze_path)}")
    

    def process_batch(batch_df: DataFrame, batch_id: int):
        count = batch_df.count()
        print(f"Processing batch_id={batch_id} with {count} rows")
        

       
        if count == 0:
            log.info(f"[Bronze] batch_id={batch_id}  EMPTY BATCH — skipping")
            return

        flagged = validate_and_flag(batch_df, amount_threshold, blacklisted)

        valid_df = flagged.filter(~F.col("is_fraud") & ~F.col("is_invalid"))
        valid_count = valid_df.count()
        log.info(f"[Bronze] batch_id={batch_id}  valid_rows={valid_count}  "
                 f"fraud/invalid={count - valid_count}")

        if valid_count == 0:
            print(f"[Bronze] batch_id={batch_id}  NO VALID ROWS — skipping write")
            return

        (
            valid_df
            .drop("is_fraud", "is_invalid", "fraud_reasons")
            .write
            .partitionBy("year", "month", "date")
            .mode("append")
            .parquet(bronze_path)
        )
        print(f"[Bronze] batch_id={batch_id}  written {valid_count} valid rows to {bronze_path}")

    return (
        raw_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{checkpoint}/bronze")
        .trigger(processingTime="10 seconds")   
        .start()
    )
    



def write_to_deadletter(raw_df: DataFrame, kafka_bootstrap: str, checkpoint: str,
                        amount_threshold: float, blacklisted: set):
    def process_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.count() == 0: return

        flagged = validate_and_flag(batch_df, amount_threshold, blacklisted)
        bad_df = flagged.filter(F.col("is_fraud") | F.col("is_invalid"))

        if bad_df.count() > 0:
            (
                bad_df
                .selectExpr("card_hash as key", "to_json(struct(*)) as value")
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_bootstrap)
                .option("topic", "payments.deadletter")
                .save()
            )


    return (
        raw_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", f"{checkpoint}/deadletter")
        .start()
    )
    

   


def main():
    print("starting streaming ETL job") 

    parser = argparse.ArgumentParser(description="Payments Streaming ETL")
    parser.add_argument("--kafka-bootstrap", default="kafka:9092")
    parser.add_argument("--bronze-path",     default="/app/data/bronze/payments")
    parser.add_argument("--checkpoint-path", default="/app/data/checkpoints")
    args = parser.parse_args()
    print(f" Args parsed — kafka={args.kafka_bootstrap}")


    spark = build_spark("PaymentsStreamingETL")
    log.info("Spark session started")
    print("=============================================")
    print(" About to call read_payments_raw...")
    raw_df = read_payments_raw(spark, args.kafka_bootstrap)
    print(" read_payments_raw returned")
    print(type(raw_df))
    print("=================================================")


    print(f"Checkpoint: Data Read success")
    print(type(raw_df))

    q1 = write_to_bronze(
        raw_df, args.bronze_path, args.checkpoint_path,
        HIGH_AMOUNT_THRESHOLD, BLACKLISTED_MERCHANTS,
    )
    q2 = write_to_deadletter(
        raw_df, args.kafka_bootstrap, args.checkpoint_path,
        HIGH_AMOUNT_THRESHOLD, BLACKLISTED_MERCHANTS,
    )
    

    log.info(f"Active queries: {[q.name for q in spark.streams.active]}")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
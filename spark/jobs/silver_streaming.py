import argparse
import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


VALID_CURRENCIES = ["USD", "EUR", "GBP", "NPR"]

def build_spark():
    return (
        SparkSession.builder
        .appName("BronzeToSilver_Batch")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def read_bronze(spark, bronze_path, processing_date):
    
    dt = datetime.strptime(processing_date, "%Y-%m-%d")
    partition_path = f"year={dt.year}/month={dt.month}/date={dt.day}"
    full_path = os.path.join(bronze_path, partition_path)
    
    if not os.path.exists(full_path):
        print(f"Warning: Path {full_path} does not exist.")
        return None
        
    return spark.read.parquet(full_path)

def process_silver(df: DataFrame):
    
    window_spec = Window.partitionBy("transaction_id").orderBy(F.col("ts_event").desc())
    df_dedup = (
        df.withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    
    df_clean = df_dedup.withColumn(
        "ts_event", F.to_timestamp("ts_event", "yyyy-MM-dd'T'HH:mm:ss'Z'")
    ).withColumn(
        "amount", F.col("amount").cast(T.DecimalType(18, 2))
    ).withColumn(
        "currency", F.upper(F.trim(F.col("currency")))
    )

    
    df_final = df_clean.withColumn(
        "silver_reject_reason",
        F.when(F.col("amount") <= 0, F.lit("INVALID_AMOUNT"))
        .when(~F.col("currency").isin(VALID_CURRENCIES), F.lit("UNSUPPORTED_CURRENCY"))
        .otherwise(F.lit(None))
    ).withColumn(
        "processed_at", F.current_timestamp()
    )

    return df_final

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--processing-date", required=True) 
    args = parser.parse_args()

    spark = build_spark()
    
    raw_df = read_bronze(spark, args.bronze_path, args.processing_date)
    
    if raw_df:
        silver_df = process_silver(raw_df)
        
        
        output_path = os.path.join(args.silver_path, f"event_date={args.processing_date}")
        
        silver_df.write \
            .mode("overwrite") \
            .parquet(output_path)
            
        print(f"Successfully processed {silver_df.count()} records to Silver.")
    
    spark.stop()

if __name__ == "__main__":
    main()
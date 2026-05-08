import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("SilverETL")

VALID_CURRENCIES  = ["USD", "EUR", "GBP", "NPR"]   
SILVER_JOB_VERSION = "1.0.0"
AMOUNT_PRECISION   = (18, 2)                        
 
CRITICAL_COLUMNS  = [                              
    "transaction_id",
    "card_hash",
    "amount",
    "event_time",
    "currency",
]
 
STRING_COLUMNS    = [                              
    "merchant_id",
    "channel",
    "auth_result",
    "location",
    "country",
]
 

def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def read_bronze(spark: SparkSession, bronze_path: str, year: int = None, month: int = None, day: int = None) -> DataFrame:
    
    if year and month and day:
        path = f"{bronze_path}/year={year}/month={month}/day={day}"
        log.info(f"Reading Bronze partition: {path}")
    else:
        path = bronze_path
        log.info(f"Reading full Bronze path: {path}")
 
    df = spark.read.parquet(path)
    log.info(f"Bronze records loaded: ")
    return df

def add_partition_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("year",  F.year("event_time"))
          .withColumn("month", F.month("event_time"))
          .withColumn("day",   F.dayofmonth("event_time"))
    )

def duplicate(df: DataFrame) -> DataFrame:
   
    
 
    w = Window.partitionBy("transaction_id").orderBy("event_time")
    df = (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
 
    
    return df

def cast_and_standardize(df: DataFrame) -> DataFrame:
    precision, scale = AMOUNT_PRECISION
 
    df = (
        df
        
        .withColumn("amount", F.col("amount").cast(T.DecimalType(precision, scale)))
 
        .withColumn("event_time", F.col("event_time").cast(T.TimestampType()))
 
       
        .withColumn("mcc", F.col("mcc").cast(T.IntegerType()))
 
        
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    )
 
    log.info("Data types cast and standardized.")
    return df

def clean_fields(df: DataFrame) -> DataFrame:
    
    df = df.dropna(subset=CRITICAL_COLUMNS)
    
    log.info(f"Null check:  records dropped for missing critical fields.")
 
    
    for col in STRING_COLUMNS:
        if col in df.columns:
            df = df.withColumn(col, F.trim(F.col(col)))
 
    
    for col in STRING_COLUMNS:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.when(F.col(col) == "", None).otherwise(F.col(col))
            )
 
   
    df = df.withColumn("auth_result", F.upper(F.col("auth_result")))
 
    log.info("String fields cleaned and normalized.")
    return df

def write_silver(df: DataFrame, silver_path: str) -> None:
    
    
    (
        df.write
        .mode("append")
        .partitionBy("year", "month", "day")
        .parquet(silver_path)
    )
    log.info(f"Silver write complete records written to {silver_path}")


def main_pipeline(bronze_path: str, silver_path: str, year: int = None, month: int = None, day: int = None):
    
    spark = build_spark("PaymentsSilverETL")
 
    log.info("========== Silver ETL Job Started ==========")
 
    
    df = read_bronze(spark, bronze_path, year, month, day)

    df = add_partition_columns(df)
 
    
    df = duplicate(df)
 
    
    df = cast_and_standardize(df)
 
    df = clean_fields(df)
 
    
    write_silver(df, silver_path)
 
    log.info("========== Silver ETL Job Completed ==========")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze → Silver Batch ETL Job")
    parser.add_argument("--bronze-path",  default="C:/data/bronze/payments",  help="Input Bronze Parquet path")
    parser.add_argument("--silver-path",  default="C:/data/silver/payments",  help="Output Silver Parquet path")
    parser.add_argument("--year",         type=int, default=None,             help="Partition year  (optional, for incremental runs)")
    parser.add_argument("--month",        type=int, default=None,             help="Partition month (optional, for incremental runs)")
    parser.add_argument("--day",          type=int, default=None,             help="Partition day   (optional, for incremental runs)")
    args = parser.parse_args()
 
    main_pipeline(
        bronze_path=args.bronze_path,
        silver_path=args.silver_path,
        year=args.year,
        month=args.month,
        day=args.day,
    )
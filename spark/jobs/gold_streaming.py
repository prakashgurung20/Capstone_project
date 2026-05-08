import argparse
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("GoldETL")


HIGH_AMOUNT_THRESHOLD = 10_000.00
BLACKLISTED_MERCHANTS = ["M11111", "M22222"]  
GOLD_JOB_VERSION      = "1.0.0"



def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("PaymentsGoldETL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")    
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .getOrCreate()
    )



def read_silver(spark: SparkSession, silver_path: str, year: int = None, month: int = None, day: int = None) -> DataFrame:
    
    if year and month and day:
        path = f"{silver_path}/year={year}/month={month}/day={day}"
        log.info(f"Reading Silver partition: {path}")
    else:
        path = silver_path
        log.info(f"Reading full Silver path: {path}")

    df = spark.read.parquet(path)
    log.info(f"Silver records loaded: {df.count()}")
    return df



def fraud_flags(df: DataFrame) -> DataFrame:
  
    df = (
        df
        .withColumn("is_high_amount",   F.col("amount") > HIGH_AMOUNT_THRESHOLD)
        .withColumn("is_blacklisted",   F.col("merchant_id").isin(BLACKLISTED_MERCHANTS))
        .withColumn("is_high_velocity", F.lit(False))   
        .withColumn("is_cross_border",  F.lit(False))   
    )
    log.info("Fraud flags derived from Silver fields.")
    return df



def build_dim_date(df: DataFrame) -> DataFrame:
    
    return (
        df
        .select(F.to_date("event_time").alias("full_date"))
        .dropDuplicates()
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year",     F.year("full_date"))
        .withColumn("month",    F.month("full_date"))
        .withColumn("day",      F.dayofmonth("full_date"))
        .select("date_key", "full_date", "year", "month", "day")
    )


def build_dim_card(df: DataFrame) -> DataFrame:
    
    return (
        df
        .select("card_hash")
        .dropDuplicates()
    )


def build_dim_merchant(df: DataFrame) -> DataFrame:

   
    return (
        df
        .select("merchant_id", "mcc", "country")
        .dropDuplicates()
    )



def build_fact_transactions(df: DataFrame) -> DataFrame:
    
    return (
        df
        .withColumn("date_key", F.date_format("event_time", "yyyyMMdd").cast("int"))
        .select(
            "transaction_id",
            "date_key",
            "card_hash",
            "merchant_id",
            "amount",
            "currency",
            "channel",
            "auth_result",
            "event_time",
        )
    )


def build_fact_settlement_daily(df: DataFrame) -> DataFrame:
    
    return (
        df
        .withColumn("date_key", F.date_format("event_time", "yyyyMMdd").cast("int"))
        .groupBy("date_key", "merchant_id")
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("*").alias("txn_count"),
            F.avg("amount").alias("avg_amount"),          
            F.max("amount").alias("max_amount"),         
        )
    )


def build_fact_fraud_signals(df: DataFrame) -> DataFrame:
    
    return (
        df
        .withColumn("date_key", F.date_format("event_time", "yyyyMMdd").cast("int"))
        .filter(
            F.col("is_high_amount") | F.col("is_blacklisted")
            
        )
        .select(
            "transaction_id",
            "date_key",
            "merchant_id",
            "is_high_amount",
            "is_blacklisted",
            "is_high_velocity",
            "is_cross_border",
        )
    )



def write_postgres(df: DataFrame, table: str, jdbc_url: str, user: str, password: str) -> None:
    
    count = df.count()

    (
        df.write
        .format("jdbc")
        .option("url",      jdbc_url) 
        .option("dbtable",  table)
        .option("user",     user)
        .option("password", password)
        .option("driver",   "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    log.info(f"Written {count} records to Postgres table: {table}")



def main():
    parser = argparse.ArgumentParser(description="Silver → Gold Batch ETL Job")
    parser.add_argument("--silver-path", required=True,       help="Input Silver Parquet path")
    parser.add_argument("--jdbc-url",    required=True,       help="Postgres JDBC URL e.g. jdbc:postgresql://localhost:5432/payments_db")
    parser.add_argument("--user",        required=True,       help="Postgres username")
    parser.add_argument("--password",    required=True,       help="Postgres password")
    parser.add_argument("--year",        type=int,            help="Partition year  (optional, for incremental runs)")
    parser.add_argument("--month",       type=int,            help="Partition month (optional, for incremental runs)")
    parser.add_argument("--day",         type=int,            help="Partition day   (optional, for incremental runs)")
    args = parser.parse_args()

    spark = build_spark()
    log.info("========== Gold ETL Job Started ==========")

    
    df = read_silver(spark, args.silver_path, args.year, args.month, args.day)

    
    df = fraud_flags(df)

   
    df.cache()
    log.info("Silver DataFrame cached for multi-table build.")

  
    log.info("--- Building Dimensions ---")
    write_postgres(build_dim_date(df),     "dim_date",     args.jdbc_url, args.user, args.password)
    write_postgres(build_dim_card(df),     "dim_card",     args.jdbc_url, args.user, args.password)
    write_postgres(build_dim_merchant(df), "dim_merchant", args.jdbc_url, args.user, args.password)

  
    log.info("--- Building Facts ---")
    write_postgres(build_fact_transactions(df),     "fact_transactions",     args.jdbc_url, args.user, args.password)
    write_postgres(build_fact_settlement_daily(df), "fact_settlement_daily", args.jdbc_url, args.user, args.password)
    write_postgres(build_fact_fraud_signals(df),    "fact_fraud_signals",    args.jdbc_url, args.user, args.password)

    df.unpersist()
    log.info("========== Gold ETL Job Completed ==========")
    spark.stop()


if __name__ == "__main__":
    main()
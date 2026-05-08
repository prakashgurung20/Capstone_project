from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckSilverSchema")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet(
    r"C:\Users\lenovo\Desktop\Capstone_project_PRD\data\silver\payments"
)

print("\n=== SILVER SCHEMA ===")
df.printSchema()

print("\n=== SAMPLE ROWS ===")
df.show(5, truncate=False)

spark.stop()
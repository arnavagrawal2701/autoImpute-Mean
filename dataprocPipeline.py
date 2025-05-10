from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, isnan, when, count

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("BigQueryAutoImpute") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.26.0") \
    .config("temporaryGcsBucket", "temp--1") \
    .getOrCreate()

# Read from BigQuery
table_id = "fifth-sol-453404-c0.my_dataset.try_data"
df = spark.read.format("bigquery").option("table", table_id).option("maxParallelism", 1).load()

# Identify numeric columns with nulls or NaNs
null_counts = df.select([
    count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in df.columns
]).collect()[0].asDict()

numeric_cols_with_nulls = [
    col_name for col_name, null_count in null_counts.items()
    if null_count > 0 and dict(df.dtypes)[col_name] in ["int", "bigint", "double", "float"]
]

# Perform mean imputation
for col_name in numeric_cols_with_nulls:
    mean_value = df.select(avg(col(col_name))).collect()[0][0]
    df = df.fillna({col_name: mean_value})

# Show result
df.write.format("bigquery") \
  .option("table", "fifth-sol-453404-c0.my_dataset.imputed_data") \
  .option("temporaryGcsBucket", "temp--1") \
  .mode("overwrite") \
  .save()

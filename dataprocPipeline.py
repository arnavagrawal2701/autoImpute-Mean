from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, isnan, when, count

# Initialize Spark session with BigQuery connector
spark = SparkSession.builder \
    .appName("BigQueryAutoImpute") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.26.0") \
    .config("temporaryGcsBucket", "<TEMP_BUCKET_NAME>") \
    .getOrCreate()

# Read data from BigQuery table
df = spark.read.format("bigquery") \
    .option("table", "<PROJECT_ID>.<DATASET>.<SOURCE_TABLE>") \
    .option("maxParallelism", 1) \
    .load()

# Identify numeric columns with null or NaN values
null_counts = df.select([
    count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in df.columns
]).collect()[0].asDict()

numeric_cols_with_nulls = [
    col_name for col_name, null_count in null_counts.items()
    if null_count > 0 and dict(df.dtypes)[col_name] in ["int", "bigint", "double", "float"]
]

# Replace nulls in numeric columns with their mean
for col_name in numeric_cols_with_nulls:
    mean_value = df.select(avg(col(col_name))).collect()[0][0]
    df = df.fillna({col_name: mean_value})

# Write the cleaned DataFrame back to BigQuery
df.write.format("bigquery") \
  .option("table", "<PROJECT_ID>.<DATASET>.<DEST_TABLE>") \
  .option("temporaryGcsBucket", "<TEMP_BUCKET_NAME>") \
  .mode("overwrite") \
  .save()

import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_BUCKET', 'SOURCE_KEY', 'DEST_BUCKET'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Construct S3 paths
source_path = f"s3://{args['SOURCE_BUCKET']}/{args['SOURCE_KEY']}"
dest_path = f"s3://{args['DEST_BUCKET']}/processed/"

# Read CSV data from source bucket
df = spark.read.json(source_path)

# Write the cleaned data to destination bucket in Parquet format
df.write.mode("overwrite").parquet(dest_path)

print(f"✅ Data processed and written to {dest_path}")

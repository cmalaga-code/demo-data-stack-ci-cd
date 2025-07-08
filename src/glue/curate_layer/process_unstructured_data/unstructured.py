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

binary_df = spark.read.format("binaryFile").load(source_path)

# Write the binary content to another S3 location
binary_df.write.mode("overwrite").format("binaryFile").save(dest_path)
print(f"✅ Data processed and written to {dest_path}")
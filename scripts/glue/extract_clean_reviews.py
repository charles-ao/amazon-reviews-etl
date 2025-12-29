import sys, io, gzip, tarfile, hashlib
import datetime
import boto3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, length, when, trim, sha2, concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Glue passes args like --BUCKET --PROJECT
args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
BUCKET = args.get("--BUCKET")
PROJECT = args.get("--PROJECT")
RAW_KEY = f"{PROJECT}/raw/amazon_review_full_csv.tgz"
RAW_PATH = f"s3://{BUCKET}/{RAW_KEY}"
INGEST_DT = datetime.datetime.now().strftime("%Y-%m-%d")

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("_c0", IntegerType(), True),
    StructField("_c1", StringType(), True),
    StructField("_c2", StringType(), True),
])


s3 = boto3.client("s3")
obj = s3.get_object(Bucket=BUCKET, Key=RAW_KEY)
body = obj["Body"].read()

tar = tarfile.open(fileobj=gzip.GzipFile(fileobj=io.BytesIO(body)))

def extract_tar(member_name):
    member = tar.getmember(member_name)
    f = tar.extractfile(member)
    data = f.read()
    tmp_path = f"/tmp/{member_name.split('/')[-1]}"
    with open(tmp_path, "wb") as out:
        out.write(data)
    df = spark.read.csv(tmp_path, schema=schema, header=False, multiLine=True, escape='"', quote='"')
    return df

def processDF(dataframe):
    data = dataframe.withColumn("title", when(col("title").isNull(), "no title").otherwise(trim(col("title")))) \
        .withColumn("rating", trim(col("rating"))) \
        .withColumn("review", trim(col("review"))) \
        .withColumn("review_length", length(col("review"))) \
        .withColumn("ingestion_date", lit(INGEST_DT)) \
        .withColumn("sentiment", when(col("rating").isin([1,2]), lit("Negative")) \
            .when(col("rating") == 3, lit("Neutral")) .otherwise(lit("Positive"))) \
        .withColumn("review_id",sha2(concat_ws("||", col("title"), col("review")), 256))

    return data

train_df = extract_tar("amazon_review_full_csv/train.csv") \
    .withColumnRenamed("_c0", "rating") \
    .withColumnRenamed("_c1", "title") \
    .withColumnRenamed("_c2", "review") \
    .withColumn("label", lit("train"))

test_df = extract_tar("amazon_review_full_csv/test.csv") \
    .withColumnRenamed("_c0", "rating") \
    .withColumnRenamed("_c1", "title") \
    .withColumnRenamed("_c2", "review") \
    .withColumn("label", lit("test"))

result_df = train_df.unionByName(test_df)

result_df = processDF(result_df)

out_path = f"s3://{BUCKET}/{PROJECT}/curated/reviews/"
result_df.write.mode("overwrite") \
  .partitionBy("split", "label") \
  .parquet(out_path)
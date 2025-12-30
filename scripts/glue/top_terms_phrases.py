import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, split, explode, lit, count, desc, concat_ws, when
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import size, sequence, expr, element_at



args = dict(zip(sys.argv[1::2], sys.argv[2::2]))
BUCKET = args.get("--BUCKET")
PROJECT = args.get("--PROJECT")
INGEST_DT = datetime.datetime.now().strftime("%Y-%m-%d")
FILE_PATH = f"s3://{BUCKET}/{PROJECT}/processed/reviews/"


spark = SparkSession.builder.getOrCreate()

spark_df = spark.read.parquet(FILE_PATH)

# Clean review text to remove punctuations, upper case, extra space. 
clean_regex = regexp_replace(
    lower(col("review")),
    r"[^a-z0-9 ]",
    ""
)
spark_df = spark_df.withColumn("clean_review", regexp_replace(clean_regex, r" +", " "))

# Tokenize
tokens = spark_df.select("sentiment", explode(split(col("clean_review"), " ")).alias("term")) \
           .filter(col("term").isNotNull() & (col("term") != "") & (col("term").rlike("^[a-z0-9]{3,}$")))

# Top terms per sentiment
terms = tokens.groupBy("sentiment", "term").agg(count("*").alias("term_count"))

w = Window.partitionBy("sentiment").orderBy(desc("term_count"))
top_terms = terms.withColumn("rank", row_number().over(w)).filter(col("rank") <= 50) \
                 .withColumn("ingest_dt", lit(INGEST_DT))

top_terms_path = f"s3://{BUCKET}/{PROJECT}/processed/summary/top_terms/"
top_terms.write.mode("overwrite").partitionBy("sentiment").parquet(top_terms_path)


# Create Top Phrases
# Build bigrams: join adjacent tokens roughly by splitting into arrays (simple approach)


arr_df = spark_df.select("sentiment", split(col("clean_review"), " ").alias("arr")) \
           .filter(size(col("arr")) > 2)

idx_df = arr_df.select("sentiment", "arr", explode(sequence(lit(1), size(col("arr")) - 1)).alias("i"))

bigrams = idx_df.select(
    "sentiment",
    concat_ws(" ", element_at(col("arr"), col("i")), element_at(col("arr"), col("i")+1)).alias("phrase")
).filter(col("phrase").rlike("^[a-z0-9 ]+$"))

phr = bigrams.groupBy("sentiment", "phrase").agg(count("*").alias("phrase_count"))

w2 = Window.partitionBy("sentiment").orderBy(desc("phrase_count"))

top_phrases = phr.withColumn("rank", row_number().over(w2)).filter(col("rank") <= 50) \
                 .withColumn("ingest_dt", lit(INGEST_DT))

top_phrases_path = f"s3://{BUCKET}/{PROJECT}/processed/summary/top_phrases/"
top_phrases.write.mode("overwrite").partitionBy("bucket").parquet(top_phrases_path)

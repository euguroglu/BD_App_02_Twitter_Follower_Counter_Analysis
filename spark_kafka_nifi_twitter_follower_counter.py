from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Kafka Broker/Cluster Details
KAFKA_TOPIC_NAME_CONS = "twittercounter"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

#Create Spark Session to Connect Spark Cluster
spark = SparkSession \
        .builder \
        .appName("TwitterFollowerCount") \
        .master("local[*]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

#Preparing schema for tweets
schema = StructType([
    StructField("timestamp_ms", LongType()),
    StructField("text", StringType()),
    StructField("user", StructType([
        StructField("id", LongType()),
        StructField("followers_count", IntegerType())]))
])

# Setting timeParserPolicy as Legacy to get previous version timeparse behaviour
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# Setting log level to error
spark.sparkContext.setLogLevel("ERROR")

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
    .option("startingOffsets", "latest") \
    .load()

#kafka_df.printSchema()

value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))

explode_df = value_df.selectExpr("value.timestamp_ms",
                                 "value.text",
                                 "value.user.id",
                                 "value.user.followers_count")

def getTeamTag(text):
    if "Fenerbahce" in text or "Fenerbahçe" in text:
        result = "Fenerbahce"
    elif "Galatasaray" in text:
        result = "Galatasaray"
    elif "Besiktas" in text or "Beşiktaş" in text:
        result = "Besiktas"
    else:
        result = "Trabzonspor"
    return result

udfgetTeamTag = udf(lambda tag: getTeamTag(tag), StringType())

df = explode_df.select(
    col("timestamp_ms"),
    col("text"),
    col("id"),
    col("followers_count"),
    udfgetTeamTag(col("text")).alias("team")
)


console_query = df \
    .writeStream \
    .queryName("Console Query") \
    .format("console") \
    .option("truncate", "true") \
    .outputMode("append") \
    .start()

console_query.awaitTermination()

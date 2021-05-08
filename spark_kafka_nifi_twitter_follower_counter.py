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
    StructField("timestamp_ms", StringType()),
    StructField("text", StringType()),
    StructField("user", StructType([
        StructField("id", LongType()),
        StructField("followers_count", IntegerType()),
        StructField("friends_count", IntegerType()),
        StructField("statuses_count", IntegerType())]))
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
                                 "value.user.followers_count",
                                 "value.user.friends_count",
                                 "value.user.statuses_count")

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

explode_df = explode_df.withColumn("timestamp_ms", col("timestamp_ms").cast(LongType()))

df = explode_df.select(
    from_unixtime(col("timestamp_ms")/1000,"yyyy-MM-dd HH:mm:ss").alias("timestamp"),
    col("text"),
    col("id"),
    col("followers_count"),
    col("friends_count").alias("followed_count"),
    col("statuses_count").alias("tweet_count"),
    udfgetTeamTag(col("text")).alias("team")
)

df = df.select("*").withColumn("timestamp", to_timestamp(col("timestamp")))

window_count_df = df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(col("team"),
        window(col("timestamp"),"2 minutes")) \
        .agg(count("team").alias("count"))

console_query = window_count_df \
    .writeStream \
    .queryName("Console Query") \
    .format("console") \
    .option("truncate", "false") \
    .outputMode("append") \
    .trigger(processingTime="2 minutes") \
    .start()

console_query.awaitTermination()

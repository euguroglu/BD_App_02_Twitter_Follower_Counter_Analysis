from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Kafka Broker/Cluster Details
KAFKA_TOPIC_NAME_CONS = "twittercounter"
KAFKA_TOPIC2_NAME_CONS = "twittercounter2"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'


# Cassandra Cluster Details
cassandra_connection_host = "localhost"
cassandra_connection_port = "9042"
cassandra_keyspace_name = "twitter"
cassandra_table_name = "tweet_club"

# Cassandra database save foreachBatch udf function
def save_to_cassandra_table(current_df, epoc_id):
    print("Inside save_to_cassandra_table function")
    print("Printing epoc_id: ")
    print(epoc_id)

    current_df \
    .write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .option("spark.cassandra.connection.host", cassandra_connection_host) \
    .option("spark.cassandra.connection.port", cassandra_connection_port) \
    .option("keyspace", cassandra_keyspace_name) \
    .option("table", cassandra_table_name) \
    .save()
    print("Exit out of save_to_cassandra_table function")
#Create Spark Session to Connect Spark Cluster
spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("TwitterFollowerCount") \
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
    .option("failOnDataLoss", "false") \
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
    if "Fenerbahce" in text or "Fenerbah??e" in text:
        result = "Fenerbahce"
    elif "Galatasaray" in text:
        result = "Galatasaray"
    elif "Besiktas" in text or "Be??ikta??" in text:
        result = "Besiktas"
    else:
        result = "Trabzonspor"
    return result

udfgetTeamTag = udf(lambda tag: getTeamTag(tag), StringType())

explode_df = explode_df.withColumn("timestamp_ms", col("timestamp_ms").cast(LongType()))

# Write raw data into HDFS
explode_df.writeStream \
  .trigger(processingTime='2 minutes') \
  .format("parquet") \
  .option("path", "hdfs://localhost:9000/tmp/data/twitter") \
  .option("checkpointLocation", "/home/enes/Applications/data2") \
  .start()


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

# Create 2 minutes thumbling window
window_count_df = df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(col("team"),
        window(col("timestamp"),"2 minutes")) \
        .agg(count("team").alias("count"))

# Create unique primary key for cassandra table
window_count_df2 = window_count_df.withColumn("start", expr("window.start"))
window_count_df3 = window_count_df2.withColumn("end", expr("window.end")).drop("window")
window_count_df4 = window_count_df3.withColumn("id", concat(col("team"),col("start")))

# Save data to cassandra
window_count_df4 \
    .writeStream \
    .trigger(processingTime='2 minutes') \
    .outputMode("update") \
    .foreachBatch(save_to_cassandra_table) \
    .start()


kafka_df = window_count_df4.select("*")

kafka_target_df = kafka_df.selectExpr("id as key",
                                             "to_json(struct(*)) as value")

nifi_query = kafka_target_df \
        .writeStream \
        .queryName("Notification Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", KAFKA_TOPIC2_NAME_CONS) \
        .outputMode("append") \
        .option("checkpointLocation", "chk-point-dir") \
        .start()

console_query = window_count_df3 \
    .writeStream \
    .queryName("Console Query") \
    .format("console") \
    .option("truncate", "false") \
    .outputMode("append") \
    .trigger(processingTime="2 minutes") \
    .start()

console_query.awaitTermination()

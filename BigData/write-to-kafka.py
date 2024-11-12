import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

os.environ["HADOOP_NATIVE_LIB"] = "C:\\Hadoop\\lib"
os.environ["HADOOP_HOME"] = "C:\\Hadoop"
os.environ["PYSPARK_PYTHON"] = "D:\\Gp\\.venv\\Scripts\\python.exe"
os.environ["JAVA_HOME"] = "C:\\Java\\jdk-8.0.432.6-hotspot"  # Set this to your Java directory
if not os.path.exists(os.path.join(os.environ["HADOOP_HOME"], "bin", "winutils.exe")):
    raise Exception("winutils.exe not found! Please check your HADOOP_HOME.")
# # Create a Spark session
# spark = SparkSession.builder \
#     .appName("FileToKafka") \
#     .master("local[*]") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4") \
#     .getOrCreate()

# # Define the schema for your DataFrame
# schema = StructType().add("tripId", IntegerType()).add("duration", IntegerType()).add("stationId", IntegerType()).add("stationName", StringType())

# # Read data from the directory as a streaming DataFrame
# streaming_df = spark.readStream \
#     .format("json") \
#     .schema(schema) \
#     .load("file:///D:/Gp/BigData/data/")  # Use directory for streaming

# print("DataFrame loaded. Proceeding to Kafka...")

# # Select specific columns from the streaming DataFrame
# df = streaming_df.selectExpr("to_json(struct(*)) AS value")

# # Write the data to Kafka
# query = df.writeStream \
#     .format("kafka") \
#     .outputMode("append") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "railway") \
#     .option("checkpointLocation", "file:///D:/Gp/BigData/checkpoint_temp") \
#     .start()

# # Wait for the query to finish
# query.awaitTermination()


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaIntegrationTest") \
    .getOrCreate()

# Create a simple DataFrame
data = [("key1", "value1"), ("key2", "value2")]
df = spark.createDataFrame(data, ["key", "value"])

# Write to Kafka
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "railway") \
    .save()
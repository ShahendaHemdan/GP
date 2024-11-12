from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, IntegerType
import pymysql
import os

os.environ["PYSPARK_PYTHON"] = "D:\\Gp\\.venv\\Scripts\\python.exe"

# Define the function to insert into MySQL
def insert_into_phpmyadmin(batch_df, batch_id):
    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    database = "final"
    username = "root"
    password = ""
    
    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    cursor = conn.cursor()

    # Iterate over the rows in the batch DataFrame
    for row in batch_df.collect():
        # Extract the required columns from the row
        column1_value = row.duration
        column2_value = row.tripId
        column3_value = row.stationId
        column4_value = row.stationName

      

        # Prepare the SQL query to insert data into the table
        sql_query = f"INSERT INTO delay (duration, tripId,stationId,stationName) VALUES ('{column1_value}', '{column2_value}', '{column3_value}', '{column4_value}')"

        # Execute the SQL query
        cursor.execute(sql_query)

    # Commit the changes
    conn.commit()
    conn.close()

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType().add("id", IntegerType()).add("duration", IntegerType()).add("stationId", IntegerType()).add("tripId", IntegerType()).add("stationName", StringType())

# Read data from Kafka topic as a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "railway") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data"))

# Select specific columns from "data"
df = df.select("data.duration", "data.tripId", "data.stationId", "data.stationName")

# Write the streaming DataFrame to the console and apply the foreachBatch operation
query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(insert_into_phpmyadmin) \
    .start()

# Wait for the query to finish
query.awaitTermination()








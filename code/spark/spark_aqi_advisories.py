import os
import sys
import pyspark

# Windows Spark environment configurations
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["SPARK_LOCAL_DIRS"] = "C:\\tmp\\spark"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# -------------------------------------------------------------------------
# 1. Initialize Spark Session with Dynamic Kafka Package Matching PySpark
# -------------------------------------------------------------------------
spark_ver = pyspark.__version__
scala_ver = "2.13" if spark_ver.startswith("4") else "2.12"
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_{scala_ver}:{spark_ver}"

print(f"[INFO] Initializing Spark {spark_ver} with Kafka package: {kafka_package}")

spark = SparkSession.builder \
    .appName("UrbanPulseAQIAdvisories") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "localhost") \
    .config("spark.jars.packages", kafka_package) \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=C:/hadoop") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------------------------------
# 2. Schema Definition for 'urbanpulse.air_quality' Topic
# -------------------------------------------------------------------------
aqi_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("aqi", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

# -------------------------------------------------------------------------
# 3. Read Streaming Data from Kafka
# -------------------------------------------------------------------------
aqi_kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094") \
    .option("subscribe", "urbanpulse.air_quality") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON Payload & Apply Watermark
aqi_parsed = aqi_kafka_stream \
    .selectExpr("CAST(value AS STRING) as json_val") \
    .select(from_json(col("json_val"), aqi_schema).alias("data")) \
    .select("data.*") \
    .filter(col("aqi").isNotNull()) \
    #.withWatermark("timestamp", "5 minutes")

# Register Stream as a Temp View for Spark SQL
aqi_parsed.createOrReplaceTempView("aqi_stream")

# -------------------------------------------------------------------------
# 4. Read Static Zone Profile Lookup CSV
# -------------------------------------------------------------------------
zone_profile_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("data/zone_profile.csv")

zone_profile_df.createOrReplaceTempView("zone_profile")

# -------------------------------------------------------------------------
# 5. Execute Streaming SQL: 10-Minute Sliding Window Average
# -------------------------------------------------------------------------
aqi_rolling_sql = spark.sql("""
    SELECT 
        window.start AS window_start,
        window.end AS window_end,
        zone,
        ROUND(AVG(aqi), 2) AS rolling_avg_aqi
    FROM aqi_stream
    GROUP BY window(timestamp, '10 minutes', '1 minute'), zone
""")

aqi_rolling_sql.createOrReplaceTempView("aqi_rolling_avg")

# -------------------------------------------------------------------------
# 6. Stream-Static Join & Health Advisory Threshold Filter
# -------------------------------------------------------------------------
advisories_df = spark.sql("""
    SELECT 
        r.window_start,
        r.window_end,
        r.zone,
        z.zone_name,
        z.population,
        z.num_schools AS number_schools,
        r.rolling_avg_aqi,
        'HEALTH_ADVISORY: Unhealthy Air Quality Detected' AS advisory_type
    FROM aqi_rolling_avg r
    JOIN zone_profile z ON r.zone = z.zone
    WHERE r.rolling_avg_aqi > 0
""")

# -------------------------------------------------------------------------
# 7. Format JSON Output Payload for Kafka Sink
# -------------------------------------------------------------------------
advisory_kafka_output = advisories_df \
    .select(
        col("zone").alias("key"),
        to_json(struct(
            col("window_start"),
            col("window_end"),
            col("zone"),
            col("zone_name"),
            col("population"),
            col("number_schools"),
            col("rolling_avg_aqi"),
            col("advisory_type")
        )).alias("value")
    )

# -------------------------------------------------------------------------
# 8. Start Streaming Query 1: Write to Kafka Sink ('urbanpulse.health_advisories')
# -------------------------------------------------------------------------
kafka_query = advisory_kafka_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094") \
    .option("topic", "urbanpulse.health_advisories") \
    .option("checkpointLocation", "data/checkpoints/spark_aqi_advisory") \
    .outputMode("update") \
    .start()

# -------------------------------------------------------------------------
# 9. Start Streaming Query 2: Write to Local Parquet Sink
# -------------------------------------------------------------------------
parquet_query = advisories_df.writeStream \
    .format("parquet") \
    .option("path", "output/health_advisories_parquet") \
    .option("checkpointLocation", "data/checkpoints/spark_aqi_advisory_parquet") \
    .outputMode("append") \
    .start()

print("\n" + "=" * 70)
print(" URBANPULSE AQI HEALTH ADVISORY STREAMING PIPELINE IS RUNNING ")
print("=" * 70 + "\n")

# Await active stream execution
spark.streams.awaitAnyTermination()
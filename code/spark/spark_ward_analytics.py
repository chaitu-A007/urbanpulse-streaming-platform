import os
import sys

# Set HADOOP_HOME and environment paths
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['JAVA_HOME'] = r'C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot'
os.environ['PATH'] = os.environ['HADOOP_HOME'] + r'\bin;' + os.environ['JAVA_HOME'] + r'\bin;' + os.environ['PATH']

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, expr, window, sum as _sum, avg, max as _max, 
    to_json, struct, date_format, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType

def run_spark_analytics():
    # Initialize Spark Session with streaming configurations
    spark = (
        SparkSession.builder
        .appName("UrbanPulse-WardAnalytics")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
        .config("spark.hadoop.fs.native.lib", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"

    # -------------------------------------------------------------
    # 1. Ward Energy Aggregation (15-Min Tumbling Window + 45-Min Watermark)
    # -------------------------------------------------------------
    meter_schema = StructType([
        StructField("meter_id", StringType(), True),
        StructField("ward_id", StringType(), True),
        StructField("kwh_reading", DoubleType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("power_factor", DoubleType(), True),
        StructField("timestamp", LongType(), True)
    ])

    meter_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", "urbanpulse.smart_meters") \
        .option("startingOffsets", "latest") \
        .load()

    # Deserialize & convert Unix timestamp (ms) to TimestampType
    meter_parsed = meter_stream \
        .select(from_json(col("value").cast("string"), meter_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))

    # Watermark 45 minutes for late-arriving data; Tumbling Window 15 minutes
    ward_aggregated = meter_parsed \
        .withWatermark("event_time", "45 minutes") \
        .groupBy(
            window(col("event_time"), "15 minutes"),
            col("ward_id")
        ) \
        .agg(
            _sum("kwh_reading").alias("total_kwh_consumed"),
            avg("power_factor").alias("avg_power_factor"),
            _max("voltage").alias("peak_voltage")
        ) \
        .withColumn("date", date_format(col("window.start"), "yyyy-MM-dd"))

    # Sink 1A: Write Aggregates to Kafka topic `ward_energy_summary`
    kafka_output = ward_aggregated \
        .select(
            col("ward_id").alias("key"),
            to_json(struct(
                col("window.start").cast("string").alias("window_start"),
                col("window.end").cast("string").alias("window_end"),
                col("ward_id"),
                col("total_kwh_consumed"),
                col("avg_power_factor"),
                col("peak_voltage")
            )).alias("value")
        ) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("topic", "ward_energy_summary") \
        .option("checkpointLocation", "data/checkpoints/spark_kafka_ward") \
        .outputMode("update") \
        .start()

    # Sink 1B: Write Partitioned Parquet Dataset to Disk (Partitioned by ward_id and date)
    parquet_output = ward_aggregated \
        .select(
            col("window.start").cast("string").alias("window_start"),
            col("window.end").cast("string").alias("window_end"),
            col("ward_id"),
            col("date"),
            col("total_kwh_consumed"),
            col("avg_power_factor"),
            col("peak_voltage")
        ) \
        .writeStream \
        .format("parquet") \
        .option("path", "data/parquet_output/ward_energy") \
        .option("checkpointLocation", "data/checkpoints/spark_parquet_ward") \
        .partitionBy("ward_id", "date") \
        .outputMode("append") \
        .start()

    # -------------------------------------------------------------
    # 2. Streaming SQL: Rolling AQI & Static Zone Join
    # -------------------------------------------------------------
    aqi_schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("aqi", IntegerType(), True),
        StructField("timestamp", LongType(), True)
    ])

    aqi_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", "urbanpulse.air_quality") \
        .option("startingOffsets", "latest") \
        .load()

    # Apply watermark directly on the DataFrame BEFORE creating a temp view
    aqi_parsed = aqi_stream \
        .select(from_json(col("value").cast("string"), aqi_schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp")) \
        .filter(col("aqi").isNotNull()) \
        .withWatermark("event_time", "10 minutes")

    aqi_parsed.createOrReplaceTempView("aqi_stream_view")

    # Read Static Zone Profile CSV Table
    zone_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/zone_profile.csv")
    
    zone_df.createOrReplaceTempView("zone_profile_static")

    # Streaming SQL Query (Standard Spark SQL Syntax)
    advisory_sql = spark.sql("""
        SELECT 
            w.zone,
            z.zone_name,
            z.population,
            z.num_schools,
            AVG(w.aqi) AS rolling_avg_aqi,
            window.end AS advisory_time
        FROM aqi_stream_view w
        JOIN zone_profile_static z ON w.zone = z.zone
        GROUP BY w.zone, z.zone_name, z.population, z.num_schools, window(w.event_time, '10 minutes', '2 minutes')
        HAVING AVG(w.aqi) > 150
    """)

    # Sink 2: Output Health Advisories to Kafka `urbanpulse.health_advisories` (Update Mode)
    advisory_kafka_output = advisory_sql \
        .select(
            col("zone").alias("key"),
            to_json(struct("*")).alias("value")
        ) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("topic", "urbanpulse.health_advisories") \
        .option("checkpointLocation", "data/checkpoints/spark_advisory_kafka") \
        .outputMode("update") \
        .start()

    print("Spark Analytics and Streaming SQL Pipelines Running...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_spark_analytics()
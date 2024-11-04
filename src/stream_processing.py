from pyspark.sql.types import StringType, StructType, DoubleType, BooleanType, ArrayType, StructField
from pyspark.sql.functions import from_json, col, when
from pyspark.sql import SparkSession
import logging
import os


os.environ['PYSPARK_SUBMIT_ARGS'] = \
    ('--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3'
     ',org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell')


class SparkStreamProcessor:
    def __init__(self, kafka_server, topic):
        self.logger = logging.getLogger(__name__)
        self.spark = (SparkSession.builder
                      .appName('NASA NEO Stream Processing')
                      .getOrCreate())
        self.kafka_server = kafka_server
        self.topic = topic

    @staticmethod
    def define_schema():
        """Define schema for incoming asteroid data."""
        schema = StructType([
            StructField('name', StringType(), True),
            StructField('is_potentially_hazardous_asteroid', BooleanType(), True),
            StructField('estimated_diameter', StructType([
                StructField('kilometers', StructType([
                    StructField('estimated_diameter_max', DoubleType(), True),
                    StructField('estimated_diameter_min', DoubleType(), True)
                ]), True)
            ]), True),
            StructField('close_approach_data', ArrayType(StructType([
                StructField('close_approach_date', StringType(), True),
                StructField('relative_velocity', StructType([
                    StructField('kilometers_per_second', StringType(), True)  # Miss distance as a string
                ]), True),
                StructField('miss_distance', StructType([
                    StructField('kilometers', StringType(), True)  # Miss distance as a string
                ]), True)
            ]), True), True)
        ])
        return schema

    def start_stream(self):
        """Start the PySpark stream, read data from Kafka and apply transformations."""

        self.logger.info("Stream Processing: Pyspark reading from Kafka stream.")
        schema = self.define_schema()

        # Read data from kafka
        stream_df = (self.spark.readStream.format('kafka')
                     .option('kafka.bootstrap.servers', self.kafka_server)
                     .option('subscribe', self.topic)
                     .load()
                     )

        # Parse and process data
        neo_df = stream_df.select(from_json(col('value').cast('string'), schema).alias('data')).select('data.*')

        processed_df = neo_df.select(
            col("name"),
            col("close_approach_data").getItem(0)
            .getField("close_approach_date").alias("close_approach_date"),
            col("estimated_diameter.kilometers.estimated_diameter_max").alias("estimated_diameter_km_max"),
            col("estimated_diameter.kilometers.estimated_diameter_min").alias("estimated_diameter_km_min"),
            col("close_approach_data").getItem(0)
            .getField("relative_velocity")
            .getField("kilometers_per_second").cast(DoubleType()).alias("relative_velocity_km_s"),
            col("close_approach_data").getItem(0)
            .getField("miss_distance")
            .getField("kilometers").cast(DoubleType()).alias("miss_distance_km"),
            col("is_potentially_hazardous_asteroid")
        )

        # Convert the boolean label to double
        processed_df = processed_df.withColumn(
            "is_potentially_hazardous_asteroid",
            when(col("is_potentially_hazardous_asteroid"), 1.0).otherwise(0.0)
        )

        self.logger.info("Stream Processing: Pyspark outputting from Kafka stream into memory sink.")
        # Write the stream output to an in-memory table
        query = (processed_df.writeStream
                 .outputMode('append')  # Append new rows to the in-memory table
                 .format('memory')  # Use memory as the output sink
                 .queryName('streaming_data')  # Name the in-memory table
                 .trigger(processingTime='30 seconds')  # Triggers a new batch every 60 seconds
                 .start())

        query.awaitTermination()

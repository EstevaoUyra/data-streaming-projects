import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from CONSTANTS import BOOTSTRAP_SERVER

schema = StructType(
    [
        StructField("crime_id", StringType(), True),
        StructField("original_crime_type_name", StringType(), True),
        StructField("report_date", DateType(), True),
        StructField("call_date", DateType(), True),
        StructField("offense_date", DateType(), True),
        StructField("call_time", TimestampType(), True),
        StructField("call_date_time", TimestampType(), True),
        StructField("disposition", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("agency_id", IntegerType(), True),
        StructField("address_type", StringType(), True),
        StructField("common_location", StringType(), True),
    ]
)


def run_spark_job(spark):

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
        .option("subscribe", "police.calls.service")
        .option("maxOffsetsPerTrigger", 200)
        .load()
    )
    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("cast(value as string) value")

    service_table = kafka_df.select(
        psf.from_json(psf.col("value"), schema).alias("DF")
    ).select("DF.*")

    distinct_table = service_table.select("original_crime_type_name", "disposition")

    # # count the number of original crime type
    agg_df = distinct_table.groupBy("original_crime_type_name").count()
    query = (agg_df
             .writeStream
             .queryName("crime counter")
             .trigger(processingTime="10 seconds")
             .option("maxRatePerPartition", 2)
             .outputMode("complete")
             .format("console")
             .start()
             )

    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)
    radio_code_df.printSchema()
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    radio_code_df.printSchema()
    join_query = (
                service_table
                .join(radio_code_df, on="disposition")
                .writeStream
                .queryName("join disposition")
                .outputMode("append")
                .format("console")
                .start()
    )
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("KafkaSparkStructuredStreaming")
        .getOrCreate()
    )
    # spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()

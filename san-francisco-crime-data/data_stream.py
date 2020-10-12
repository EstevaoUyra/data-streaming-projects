import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from CONSTANTS import BOOTSTRAP_SERVER

schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", DateType(), True),
    StructField("call_date", DateType(), True),
    StructField("offense_date", DateType(), True),
    StructField("call_time",  TimestampType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(),True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", IntegerType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])


def run_spark_job(spark):
    df = (
        spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
        .option("subscribe", "police.calls.service")
        .option("maxOffsetsPerTrigger", 200)
        .load()
    )
    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("cast(value as string) value")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    query = (service_table
     .writeStream
     .outputMode("append")
     .format("console")
     .start()
     )

    query.awaitTermination()
    #
    # # TODO select original_crime_type_name and disposition
    # distinct_table =
    #
    # # count the number of original crime type
    # agg_df =
    #
    # # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # # TODO write output stream
    # query = agg_df \
    #
    #
    # # TODO attach a ProgressReporter
    # query.awaitTermination()
    #
    # # TODO get the right radio code json path
    # radio_code_json_filepath = ""
    # radio_code_df = spark.read.json(radio_code_json_filepath)
    #
    # # clean up your data so that the column names match on radio_code_df and agg_df
    # # we will want to join on the disposition code
    #
    # # TODO rename disposition_code column to disposition
    # radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    #
    # # TODO join on disposition column
    # join_query = agg_df.
    #
    #
    # join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()

import logging
import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from dateutil.parser import parse as parse_date
import threading
import time

# TODO Create a schema for incoming resources
# schema

schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])





# TODO create a spark udf to convert time to YYYYmmDDhh format
@psf.udf(StringType())
def udf_convert_time(timestamp):
    return str(timestamp.strftime('%Y%m%d%H'))


def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port

    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "demo").option("maxOffsetsPerTrigger",200).option("startingOffsets", "latest").load()


    # # Show schema for the incoming resources for checks
    df.printSchema()


    # # TODO extract the correct column from the kafka input resources
    kafka_df = df.selectExpr("CAST(value AS STRING)")



    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("SERVICE_CALLS"))\
        .select("SERVICE_CALLS.*")


    distinct_table = service_table\
        .select(psf.col('crime_id'),
                psf.col('original_crime_type_name'),
                psf.to_timestamp(psf.col('call_date_time')).alias('call_datetime'),
                psf.col('address'),
                psf.col('disposition'))


    # TODO get different types of original_crime_type_name in 60 minutes interval
    counts_df = (
            distinct_table\
                .withWatermark("call_datetime", "1 minutes") \
                .groupBy(
                distinct_table.original_crime_type_name,
                psf.window(distinct_table.call_datetime, "60 minutes","60 minutes"))\
                .count()
        )

    # TODO use udf to convert timestamp to right format on a call_date_time column
    converted_df = distinct_table.withColumn('YYYYmmDDhh',udf_convert_time(psf.col('call_datetime')))

    # TODO apply aggregations using windows function to see how many calls occurred in 2 day span
    calls_per_2_days =distinct_table\
                .withWatermark("call_datetime", "1 hour") \
                .groupBy(
                psf.window(distinct_table.call_datetime, "2 days"))\
                .count()
        

    # TODO write output stream

    query = (calls_per_2_days
    .writeStream
    .format("console")
    .option("truncate", False)
    .outputMode("update")
    .start())


    query_active = True
    t1 = threading.Thread(target=progressReporter, args=(query,query_active,))
    t1.start()
    
        
    # query.awaitTermination()
    spark.streams.awaitAnyTermination()
    query_active = False
    t1.join()
    

def progressReporter(query,query_active): 
    """ 
    function to print last progress of the  give query
    """ 
    while(query_active):
        if query is not None and query.status['isTriggerActive']==True:
            print(query.lastProgress)
            # println(query.status)
        time.sleep(2)
       


if __name__ == "__main__":
    
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Local mode
    spark = (SparkSession
    .builder
    .master('local[*]')
    .appName("StructuredStreamingWithKafka")
    .getOrCreate()) 

    spark.sparkContext.setLogLevel("WARN")


    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()





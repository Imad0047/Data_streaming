from pyspark.sql import SparkSession
import logging
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType , DoubleType



logging.basicConfig(level=logging.INFO)

def create_spark_connection():
    spark_conn = None

    try:
        spark_conn = SparkSession.builder \
        .appName('SparkDataStreaming') \
        .config('spark.jars.packages',"org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfuly |")
    except Exception as e :
        logging.error(f"couldn't create the spark session due to exception {e}")

    return spark_conn

def connect_to_kafak(conn):
    spark_df = None
    try:
        spark_df = conn.readStream \
         .format('kafka') \
         .option('kafka.bootstrap.servers', 'localhost:9092') \
         .option('subscribe','weather_topic') \
         .load()
        logging.info("kafka dataframe created successfuly")
    except Exception as e :
        logging.warning(f"kafka dataframe could not be created because {e}")

    return spark_df

def create_schema(spark_df):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("main", StringType(), True),
        StructField("cord", StructType([
            StructField("long",DoubleType(),True),
            StructField("lat",DoubleType(),True)
        ]))
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    return sel
    
if __name__ == "__main__":
    conn = create_spark_connection()

    if conn is not None:
        #connect kafka with spark

        spark_df = connect_to_kafak(conn)
        selection_df = create_schema(spark_df)
        query = selection_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
        query.awaitTermination()
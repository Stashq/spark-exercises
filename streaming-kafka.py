# from pyspark.sql.functions import col
# from pyspark.sql.types import DoubleType
import os
from pyspark.sql import SparkSession

os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell'
# schema = StructType([
#     StructField('timestamp', TimestampType()),
#     StructField('value', DoubleType())
# ])


spark = SparkSession.builder.master('local[1]').getOrCreate()
df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "timeseries-topic") \
        .load()


# df = df.select('value')
# df = df.withColumn("value", col('value').cast(DoubleType()))
# df = df.writeStream.format('console').start()

df.selectExpr(
    "CAST(key AS STRING)",
    "CAST(value AS STRING)"
).writeStream.format('console').start()

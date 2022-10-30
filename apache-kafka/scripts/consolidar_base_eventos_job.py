from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, expr, struct, lit, concat, array, date_format, current_timestamp
from pyspark.sql.avro.functions import to_avro

spark = SparkSession \
    .builder \
    .appName("ConsolidarBaseEventosJob") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

spark.readStream \
    .format("parquet") \
    .schema(spark.read.parquet("D:\\s3\\efinanceira-monitoracao-transmissao\\bkt-staging-data").schema) \
    .option("path", "D:\\s3\\efinanceira-monitoracao-transmissao\\bkt-staging-data") \
    .load() \
    .writeStream \
    .partitionBy("date") \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "D:\\s3\\efinanceira-monitoracao-transmissao\\bkt-raw-data") \
    .option("checkpointLocation", "D:\\s3\\efinanceira-monitoracao-transmissao\\bkt-checkpoint-data\\consolidar-base-eventos-job") \
    .trigger(once=True) \
    .start() \
    .awaitTermination()
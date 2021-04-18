from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_date
from pyspark.sql.avro.functions import from_avro
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from avro import schema

schema_registry_client = CachedSchemaRegistryClient({
    "url": "https://psrc-4j1d2.westus2.azure.confluent.cloud",
    "basic.auth.credentials.source": "USER_INFO",
    "basic.auth.user.info": "2BEQE2KDNBJGDH2Y:8nixndjUyjXqTJoXnm3X3GwLZPz5F8umq74/g9ioG2mIi4lm0CWF1nUAf8deIFbP"
})

latest_id, latest_schema, latest_version = schema_registry_client.get_latest_schema("processamento-ted-value")

spark = SparkSession \
    .builder \
    .appName("CapturarEventosJob") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

raw_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-epwny.eastus.azure.confluent.cloud:9092") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule   required username='BIMCMFF6WU3YBB34'   password='Xnr9geulvxPYeyNeL2r56iyjNG5dwkB2CTnQz+syVZwOUfJIQFxmSJT0+MskxOnQ';") \
    .option("kafka.sasl.mechanism", "PLAIN")  \
    .option("kafka.group.id", "efinanceira-monitoracao-transmissao") \
    .option("subscribe", "processamento-ted") \
    .option("startingOffsets", "earliest") \
    .option("includeHeaders", "true") \
    .load()

parsed_data = raw_data \
    .select(col("topic"),
            col("partition"),
            col("offset"),
            col("headers")[0]["value"].cast("string").alias("specversion"),
            col("headers")[1]["value"].cast("string").alias("type"),
            col("headers")[2]["value"].cast("string").alias("source"),
            col("headers")[3]["value"].cast("string").alias("id"),
            col("headers")[4]["value"].cast("string").cast("timestamp").alias("time"),
            col("headers")[5]["value"].cast("string").alias("messageversion"),
            col("headers")[6]["value"].cast("string").alias("eventversion"),
            col("headers")[7]["value"].cast("string").alias("transactionid"),
            col("headers")[8]["value"].cast("string").alias("correlationid"),
            col("headers")[9]["value"].cast("string").alias("datacontenttype"),
            from_avro(expr("substring(value, 6)"), str(latest_schema)).alias("payload")) \
    .withColumn("date", to_date(col("time"))) \
    .withWatermark("time", "2 minutes") \
    .dropDuplicates(subset=['id'])

parsed_data.printSchema()

query = parsed_data \
    .writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", True) \
    .option("checkpointLocation", "D:\\s3\\bkt-raw-data\\checkpoint") \
    .trigger(once=True) \
    .start()

query.awaitTermination()

    # .partitionBy("date") \
    # .format("parquet") \
    # .outputMode("append") \
    # .option("path","D:\\s3\\bkt-raw-data\\data") \
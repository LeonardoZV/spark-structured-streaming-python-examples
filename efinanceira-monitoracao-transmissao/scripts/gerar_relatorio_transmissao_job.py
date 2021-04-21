from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, expr, struct, lit, concat, array, date_format, current_timestamp
from pyspark.sql.avro.functions import to_avro
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient

schema_registry_client = CachedSchemaRegistryClient({
    "url": "https://psrc-4j1d2.westus2.azure.confluent.cloud",
    "basic.auth.credentials.source": "USER_INFO",
    "basic.auth.user.info": "2BEQE2KDNBJGDH2Y:8nixndjUyjXqTJoXnm3X3GwLZPz5F8umq74/g9ioG2mIi4lm0CWF1nUAf8deIFbP"
})

latest_id, latest_schema, latest_version = schema_registry_client.get_latest_schema("relatorio-transmissao-value")

magic_byte = bytes([0x0])

id_bytes = (latest_id).to_bytes(4, byteorder='big')

spark = SparkSession \
    .builder \
    .appName("GerarRelatorioTransmissaoJob") \
    .master("local[*]") \
    .getOrCreate()

sqlContext = SQLContext(spark.sparkContext)

spark.sparkContext.setLogLevel('WARN')

staging_data = spark \
    .readStream \
    .format("parquet") \
    .schema(spark.read.parquet("D:\\s3\\bkt-staging-data").schema) \
    .option("path", "D:\\s3\\bkt-staging-data") \
    .load()

staging_data.createOrReplaceTempView("evento")

aggregated_data = sqlContext.sql("SELECT payload.data.codigo_produto_operacional, COUNT(*) as quantidade_eventos_transmitidos, COUNT(case when payload.data.codigo_empresa = 341 then 1 else null end) as quantidade_eventos_transmitidos_sucesso, COUNT(case when payload.data.codigo_empresa = 350 then 1 else null end) as quantidade_eventos_transmitidos_erro FROM evento GROUP BY payload.data.codigo_produto_operacional") \
    .withColumn("data",	struct("*")) \
    .withColumn("value", concat(lit(magic_byte), lit(id_bytes), to_avro(struct("data"), str(latest_schema)))) \
    .withColumn("headers ",
        array(
            struct(lit("specversion").alias("key"), lit("1").cast("binary").alias("value")),
            struct(lit("type").alias("key"), lit("").cast("binary").alias("value")),
            struct(lit("source").alias("key"), lit("urn:sigla:gerar-relatorio-transmissao-job").cast("binary").alias("value")),
            struct(lit("id").alias("key"), expr("uuid()").cast("binary").alias("value")),
            struct(lit("time").alias("key"), date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast("binary").alias("value")),
            struct(lit("messageversion").alias("key"), lit("1").cast("binary").alias("value")),
            struct(lit("transactionid").alias("key"), lit("").cast("binary").alias("value")),
            struct(lit("correlationid").alias("key"), lit("").cast("binary").alias("value")),
            struct(lit("datacontenttype").alias("key"), lit("application/avro").cast("binary").alias("value"))
        )
    ) \
    .writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", False) \
    .option("checkpointLocation", "D:\\s3\\bkt-checkpoint-data\\gerar-relatorio-transmissao-job") \
    .trigger(once=True) \
    .start() \
    .awaitTermination()

    # .format("kafka") \
    # .outputMode("update") \
    # .option("kafka.bootstrap.servers", "pkc-epwny.eastus.azure.confluent.cloud:9092") \
    # .option("kafka.security.protocol", "SASL_SSL") \
    # .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule   required username='BIMCMFF6WU3YBB34'   password='Xnr9geulvxPYeyNeL2r56iyjNG5dwkB2CTnQz+syVZwOUfJIQFxmSJT0+MskxOnQ';") \
    # .option("kafka.sasl.mechanism", "PLAIN") \
    # .option("topic", "relatorio-transmissao") \
    # .option("includeHeaders", "true") \
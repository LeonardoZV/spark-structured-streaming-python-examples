from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

job_name = "usuarios-eventos-job"
delta_table_name = "`teste_db`.`usuario_evento_delta`"
table_name = "`teste_db`.`usuario_evento`"

spark = SparkSession.builder.appName(job_name) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("CREATE TABLE IF NOT EXISTS " + delta_table_name + " (id STRING, data_hora_evento TIMESTAMP, codigo_usuario STRING, nome_usuario STRING) USING DELTA")

spark.sql("ALTER TABLE " + delta_table_name + " SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)")

schema = StructType([
    StructField("id", StringType()),
    StructField("data_hora_evento", TimestampType()),
    StructField("codigo_usuario", StringType()),
    StructField("nome_usuario", StringType())]
)

spark.readStream.format("json") \
    .schema(schema) \
    .load("s3://aws-emr-assets-428204489288-us-east-1/samples/") \
    .dropDuplicates(subset=["id"]) \
    .writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://aws-emr-assets-428204489288-us-east-1/checkpoints/" + job_name + "/") \
    .trigger(once=True) \
    .toTable(delta_table_name) \
    .awaitTermination()

spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS " + table_name + " (id STRING, data_hora_evento TIMESTAMP, codigo_usuario STRING, nome_usuario STRING) \
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' \
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' \
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' \
LOCATION 's3://aws-emr-assets-428204489288-us-east-1/databases/teste_db/usuario_evento_delta/_symlink_format_manifest/'")
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

delta_table_name = "`teste_db`.`usuario_evento_delta`"
delta_table_path = "s3://aws-emr-assets-428204489288-us-east-1/databases/teste_db/usuario_evento_delta"

spark = SparkSession.builder.appName("usuarios-eventos-job") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("CREATE TABLE IF NOT EXISTS " + delta_table_name + " (id STRING, data_hora_evento TIMESTAMP, codigo_usuario STRING, nome_usuario STRING) USING delta TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)")

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
    .option("checkpointLocation", delta_table_path + "/_checkpoints/") \
    .trigger(once=True) \
    .toTable(delta_table_name) \
    .awaitTermination()

spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS `teste_db`.`usuario_evento` (id STRING, data_hora_evento TIMESTAMP, codigo_usuario STRING, nome_usuario STRING) \
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' \
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' \
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' \
LOCATION '" + delta_table_path + "/_symlink_format_manifest/'")
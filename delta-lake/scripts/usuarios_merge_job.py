from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, desc, row_number
from delta.tables import DeltaTable

job_name = "usuarios-merge-job"
delta_table_name = "`teste_db`.`usuario_delta`"
table_name = "`teste_db`.`usuario`"

spark = SparkSession.builder.appName(job_name) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("CREATE TABLE IF NOT EXISTS " + delta_table_name + " (codigo_usuario STRING, nome_usuario STRING) USING DELTA")

spark.sql("ALTER TABLE " + delta_table_name + " SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)")

schema = StructType([
    StructField("id", StringType()),
    StructField("data_hora_evento", TimestampType()),
    StructField("codigo_usuario", StringType()),
    StructField("nome_usuario", StringType())]
)

window = Window.partitionBy("codigo_usuario").orderBy(desc("data_hora_evento"))

deltaTable = DeltaTable.forName(spark, delta_table_name)


def upsert_to_delta(source, batch_id):
    deltaTable.alias("t").merge(source.alias("s"), "s.codigo_usuario = t.codigo_usuario") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()


spark.readStream.format("json") \
    .schema(schema) \
    .load("s3://aws-emr-assets-428204489288-us-east-1/samples/") \
    .dropDuplicates(subset=["id"]) \
    .withColumn("row", row_number().over(window)) \
    .filter(col("row") == 1) \
    .drop("row") \
    .writeStream.format("delta") \
    .foreachBatch(upsert_to_delta) \
    .outputMode("append") \
    .option("checkpointLocation", "s3://aws-emr-assets-428204489288-us-east-1/checkpoints/" + job_name + "/") \
    .trigger(once=True) \
    .start() \
    .awaitTermination()

spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS " + table_name + " (codigo_usuario STRING, nome_usuario STRING) \
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' \
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' \
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' \
LOCATION 's3://aws-emr-assets-428204489288-us-east-1/databases/teste_db/usuario_delta/_symlink_format_manifest/'")

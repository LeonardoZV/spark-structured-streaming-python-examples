# Spark Structured Streaming Python Examples

## Apache Kafka
Examples of data consumption and production of events in Kafka with Schema Registry, using Spark Structured Streaming.

Este exemplo demonstra como utilizar o Kafka como origem para popular, de forma simultânea, um lake de dados e gerar relatórios agregados em near real time.

![efinanceira-monitoracao-transmissao](apache-kafka/media/efinanceira-monitoracao-transmissao.png)

1. **CapturarEventosJob:** Consome eventos e os armazenada em formato parquet em um bucket de staging. Por se tratar de consumo de streamig, será gerado um arquivo parquet para cada consumidor/partição e micro batch.
2. **ConsolidarBaseEventosJob:** Consome os vários arquivos em formato parquet, os consolida na maneira que faça mais sentido para otimizar a consulta, armazenando o resultado em um bucket raw.
3. **GerarRelatorioTransmissaoJob:** Consome os vários arquivos em formato parquet, os agrega conforme necessário pelo relatório e realiza a postagem no kafka.

**Requisitos:**
```
pip install -r requirements.txt 
```

**Executando:**
```
<path_spark>\bin\spark-submit --packages org.apache.spark:spark-core_2.12:3.1.1,org.apache.spark:spark-sql_2.12:3.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-avro_2.12:3.1.1 <path_script>\capturar_eventos_job.py
<path_spark>\bin\spark-submit --packages org.apache.spark:spark-core_2.12:3.1.1,org.apache.spark:spark-sql_2.12:3.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-avro_2.12:3.1.1 <path_script>\consolidar_base_eventos_job.py
<path_spark>\bin\spark-submit --packages org.apache.spark:spark-core_2.12:3.1.1,org.apache.spark:spark-sql_2.12:3.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-avro_2.12:3.1.1 <path_script>\gerar_relatorio_transmissao_job.py
```

## Delta Lake
Examples of data consumption and production in Delta Lake, using Spark Structured Streaming.

![delta-lake-example-architecture](delta-lake/media/delta-lake-example-architecture.png)

**Scripts:**

usuario_evento_job.py: Consumes events stored as parquet files in a samples bucket and saves them in a delta table as a event, appending only.

usuario_merge_job.py: Consumes events stored as parquet files in a samples bucket and merges them in a delta table.

Both scripts first registers the delta tables in the catalog, generate those tables manifest files and create a virtual table in the catalog from the manifest files so it can be queryable by tools like Athena, Presto and Trino.

The use of the trigger(once=True) feature is important as a cost saving if you don't need near real time Analytics.

**Requirements:**
```
pip install -r requirements.txt 
```

**Execution:**
```
<path_spark>\bin\spark-submit --packages io.delta:delta-core_2.12:2.1.1 <path_script>\capturar_eventos_job.py
<path_spark>\bin\spark-submit --packages io.delta:delta-core_2.12:2.1.1 <path_script>\consolidar_base_eventos_job.py
<path_spark>\bin\spark-submit --packages io.delta:delta-core_2.12:2.1.1 <path_script>\gerar_relatorio_transmissao_job.py
```
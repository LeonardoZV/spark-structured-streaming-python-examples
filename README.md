# Spark Streaming Kafka Python Exemples
Examples of consuming and producing events in Kafka (+ Schema Registry) using Spark Streaming.

## e-Financeira Transmission Monitoring Tool

### What is it?
e-Financeira is a accessory obligation that banks in Brazil have to report to Receita Federal do Brasil, the Brazilian federal revenue service.

### What we need to do?
Users need two things:

1. View events that were transmitted to RFB with success or error, in detail.
2. View a aggregated report with the sum of events categorized by success or error by company.

### Architecture
This example demonstrates how to use Kafka as a source to simultaneously populate a data lake and generate a aggregated report in near real time.

![efinanceira-monitoracao-transmissao](efinanceira-monitoracao-transmissao/media/efinanceira-monitoracao-transmissao.png)

1. **CapturarEventosJob:** Consumes events and stores them in parquet format in a Source of Record bucket. Because it is a streamig consumption, a parquet file will be generated for each consumer/partition and micro batch.

2. **ConsolidarBaseEventosJob:** It consumes the files in the Source of Record bucket, consolidates them in a way that makes the most sense to optimize the queries and stores the result in a Source of Truth bucket.

3. **GerarRelatorioTransmissaoJob:** It consumes the files in the Source of Record bucket, aggregates them as needed by the report and performs the posting to kafka.

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

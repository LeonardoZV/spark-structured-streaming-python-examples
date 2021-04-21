# Spark Streaming Kafka Python Exemplos
Exemplos de consumo e produção de eventos no Kafka (+ Schema Registry) utilizando Spark Streaming.

## eFinanceira Monitoração Transmissão
Este exemplo demonstra como utilizar o Kafka como origem para popular, de forma simultânea, um lake de dados e gerar relatórios agregados em near real time.

![efinanceira-monitoracao-transmissao](efinanceira-monitoracao-transmissao/media/efinanceira-monitoracao-transmissao.png)

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
<path_spark>\bin\spark-submit --packages org.apache.spark:spark-core_2.12:3.1.1,org.apache.spark:spark-sql_2.12:3.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.spark:spark-avro_2.12:3.1.1 <path_script>\gerar_relatorio_transmissao_job.py
```

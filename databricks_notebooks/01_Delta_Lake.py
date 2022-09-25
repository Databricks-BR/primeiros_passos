# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: left; valign: top; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/databricks_br_header.png" alt="GitHub Databricks Brasil" style="width: 700px"><a href="https://github.com/Databricks-BR"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_github.png" style="width: 40px; height: 40px;"></a>
# MAGIC    <a href="https://github.com/Databricks-BR"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/databricks-br.png" style="width: 40px; height: 40px;"></a>  <a href="https://www.linkedin.com/groups/14100135"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_linkedin.png" style="width: 35px; height: 35px;"></a>  <a href="https://www.meetup.com/pt-BR/databricks-brasil-oficial"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_meetup.png" style="height: 40px;"></a>  <a href="https://bit.ly/databricks-slack-br"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_slack.png" style="width: 35px; height: 35px;"></a>  <a href="https://www.youtube.com/c/Databricks"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_youtube.png" style="height: 38px;"></a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: left; valign: top; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/logo_delta_lake.png" style="width: 250px;">
# MAGIC </div>
# MAGIC 
# MAGIC   
# MAGIC O **Delta Lake** é uma camada de armazenamento de código aberto que adiciona confiabilidade, desempenho, governança e qualidade aos Data Lakes existentes — para operações de streaming e em lote (_BATCH_), baseado em um padrão aberto (_open format_): "**PARQUET**".
# MAGIC O Delta habilita o paradigma **LAKEHOUSE** que combina os pontos fortes de Data lakes + Data Warehouses.
# MAGIC Ao substituir os silos de dados por um único local para dados: estruturados, semiestruturados e não estruturados; o Delta Lake é a base de um LAKEHOUSE econômico e altamente escalável.
# MAGIC 
# MAGIC ##### Referência:
# MAGIC * https://learn.microsoft.com/pt-br/azure/databricks/delta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação do Database (_Schema_)
# MAGIC Sintaxe na linguagem SQL:
# MAGIC 
# MAGIC ```SQL
# MAGIC CREATE DATABASE IF NOT EXISTS <nome_do_database>;ˋ
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS <nome_do_database> LOCATION '${da.paths.working_dir}/_custom_location.db';ˋ
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS DATABASE_NAME;
# MAGIC 
# MAGIC USE DATABASE_NAME;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação das primeiras tabelas (DELTA TABLE)
# MAGIC 
# MAGIC Sintaxe na linguagem SQL:
# MAGIC 
# MAGIC ```SQL
# MAGIC CREATE TABLE IF NOT EXISTS nome_tabela  (campo1 INT, campo2 STRING, campo3 DOUBLE, ...);
# MAGIC ```
# MAGIC 
# MAGIC ##### Referência:
# MAGIC * https://learn.microsoft.com/pt-br/azure/databricks/delta/quick-start#sql

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS nome_tabela
# MAGIC AS SELECT * FROM delta.`/databricks-datasets/learning-spark-v2/people/people-10m.delta`;

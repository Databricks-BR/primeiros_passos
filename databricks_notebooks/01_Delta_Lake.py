# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: left; valign: top; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/databricks_br_header.png" alt="GitHub Databricks Brasil" style="width: 700px"><a href="https://github.com/Databricks-BR"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_github.png" style="width: 40px; height: 40px;"></a>
# MAGIC    <a href="https://github.com/Databricks-BR"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/databricks-br.png" style="width: 40px; height: 40px;"></a>  <a href="https://www.linkedin.com/groups/14100135"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_linkedin.png" style="width: 35px; height: 35px;"></a>  <a href="https://www.meetup.com/pt-BR/databricks-brasil-oficial"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_meetup.png" style="height: 40px;"></a>  <a href="https://bit.ly/databricks-slack-br"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_slack.png" style="width: 35px; height: 35px;"></a>  <a href="https://www.youtube.com/c/Databricks"><img src="https://raw.githubusercontent.com/Databricks-BR/Databricks-BR/main/images/icon_youtube.png" style="height: 38px;"></a>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação do Database (_Schema_)
# MAGIC ```SQL
# MAGIC CREATE DATABASE IF NOT EXISTS ${da.db_name}_default_location;ˋ
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS ${da.db_name}_custom_location LOCATION '${da.paths.working_dir}/_custom_location.db';ˋ
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS DATABASE_NAME;
# MAGIC 
# MAGIC USE DATABASE_NAME;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação das primeiras tabelas (DELTA TABLE)
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

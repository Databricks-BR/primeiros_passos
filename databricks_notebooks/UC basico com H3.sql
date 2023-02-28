-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC ### Governança de Dados - Unity Catalog
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-base-1.png" style="float: right" width="800px"/> 
-- MAGIC 
-- MAGIC The first step is to create a new catalog.
-- MAGIC 
-- MAGIC Unity Catalog works with 3 layers:
-- MAGIC 
-- MAGIC * CATALOG
-- MAGIC * SCHEMA (or DATABASE)
-- MAGIC * TABLE
-- MAGIC 
-- MAGIC To access one table, you can specify the full path: ```SELECT * FROM <CATALOG>.<SCHEMA>.<TABLE>```
-- MAGIC 
-- MAGIC Note that the tables created before Unity Catalog are saved under the catalog named `hive_metastore`. Unity Catalog features are not available for this catalog.

-- COMMAND ----------

-- DBTITLE 1,Criação do Catálogo 
CREATE CATALOG IF NOT EXISTS luis_assuncao;

USE CATALOG luis_assuncao;

-- COMMAND ----------

-- DBTITLE 1,Criação do Database - SCHEMA
CREATE SCHEMA IF NOT EXISTS  ESG;

-- COMMAND ----------

-- DBTITLE 1,Criação da Tabela - Dados Pluviométricos - Camada Bronze (RAW DATA)
CREATE TABLE IF NOT EXISTS
  ESG.bronze_pluviometria 
  (
    latitude      DOUBLE COMMENT 'Latitude Geografica' ,
    longitude     DOUBLE COMMENT 'Longitude Geografica',
    pluviometria  INT    COMMENT 'Indice Pluviometrico'
  );

INSERT INTO ESG.bronze_pluviometria 
    (latitude, longitude, pluviometria)
VALUES
    (22.9068, 43.1729, 10),
    (23.5558, 46.6396, 20),
    (17.9302, 43.7908,  5);


-- COMMAND ----------

SELECT * FROM ESG.bronze_pluviometria ;

-- COMMAND ----------

-- DBTITLE 1,Criação da Tabela - Dados de Clima - Camada Bronze (RAW DATA)
CREATE TABLE IF NOT EXISTS
  ESG.bronze_clima 
  (
    latitude      DOUBLE COMMENT 'Latitude Geografica' ,
    longitude     DOUBLE COMMENT 'Longitude Geografica',
    temperatura   INT    COMMENT 'Temperatura em Graus Celcius'
  );

INSERT INTO ESG.bronze_clima 
    (latitude, longitude, temperatura)
VALUES
    (22.9068, 43.1729, 38),
    (23.5558, 46.6396, 20),
    (17.9302, 43.7908, 18);

-- COMMAND ----------

SELECT * FROM ESG.bronze_clima 

-- COMMAND ----------

-- DBTITLE 1,Instalação da Biblioteca H3
-- MAGIC %python
-- MAGIC %pip install h3==3.6.3

-- COMMAND ----------

-- DBTITLE 1,Biblioteca de GeoReferencia
-- MAGIC %python
-- MAGIC import h3
-- MAGIC from pyspark.sql.functions import udf
-- MAGIC from pyspark.sql import functions as F
-- MAGIC 
-- MAGIC @udf("string")
-- MAGIC def to_h3(lat, lng, precision):
-- MAGIC   h = h3.geo_to_h3(lat, lng, precision)
-- MAGIC   return h.upper()

-- COMMAND ----------

-- DBTITLE 1,Transformando a Tabela Clima -->. SILVER Layer
-- MAGIC %python
-- MAGIC df = spark.read.table('ESG.bronze_clima') \
-- MAGIC       .select( to_h3(F.col("latitude"), F.col("longitude"), F.lit(11)).alias("geo_h3"), F.col("temperatura"))
-- MAGIC 
-- MAGIC # Create table
-- MAGIC df.write.format("delta").mode("overwrite").saveAsTable("ESG.silver_clima")

-- COMMAND ----------

-- DBTITLE 1,Transformando a Tabela Pluviometria -->. SILVER Layer
-- MAGIC %python
-- MAGIC df = spark.read.table('ESG.bronze_pluviometria') \
-- MAGIC       .select( to_h3(F.col("latitude"), F.col("longitude"), F.lit(11)).alias("geo_h3"), F.col("pluviometria"))
-- MAGIC 
-- MAGIC # Create table
-- MAGIC df.write.format("delta").mode("overwrite").saveAsTable("ESG.silver_pluviometria")

-- COMMAND ----------

SELECT P.geo_h3, P.pluviometria, C.temperatura, (P.pluviometria + C.temperatura) as score
FROM ESG.silver_pluviometria P
INNER JOIN ESG.silver_clima C
WHERE P.geo_h3 = C.geo_h3

-- COMMAND ----------

CREATE TABLE ESG.gold_indicadores
AS
SELECT P.geo_h3, (P.pluviometria + C.temperatura) as score
FROM ESG.silver_pluviometria P
INNER JOIN ESG.silver_clima C
WHERE P.geo_h3 = C.geo_h3

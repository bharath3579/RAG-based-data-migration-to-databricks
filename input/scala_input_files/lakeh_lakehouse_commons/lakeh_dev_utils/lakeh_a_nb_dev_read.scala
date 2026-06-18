// Databricks notebook source
// MAGIC %md
// MAGIC # Notebook for querying data contained in the DL
// MAGIC This notebook is designed to obtain the necessary data for tests.

// COMMAND ----------

// MAGIC %run /Shared/DAB-PAN0001/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_a_nb_arquetipo_functions

// COMMAND ----------


val s_eObject = getObject(source_object)

val df = if (source_format.equalsIgnoreCase("table") )
              {spark.read.table(s_eObject)}
                
               else {
                spark.read
                .format(source_format)
                .load(s_eObject)}
df.createOrReplaceTempView("tabla")
/* Esta query se puede modificar para obtener los datos que necesitamos para los test*/
val query = s"SELECT * FROM tabla WHERE 1=1 LIMIT $limit"

/* Cuando hacemos un display podemos exportar los datos como csv o como json */
display(spark.sql(query))

// COMMAND ----------

// MAGIC %md
// MAGIC # Instructions for exportation
// MAGIC ## CSV:
// MAGIC The header should not be removed.
// MAGIC Check that the text fields do not contain the delimiter. Otherwise, the file will need to be modified to change the delimiter.
// MAGIC ## JSON
// MAGIC The fields structure must be delimited.
// MAGIC The rows field must be removed, preserving its value, a list with all the values ​​in the dataset.
// MAGIC Multi-line JSON is allowed.

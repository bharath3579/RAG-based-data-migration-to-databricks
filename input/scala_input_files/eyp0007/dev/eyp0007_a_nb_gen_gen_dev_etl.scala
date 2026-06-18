// Databricks notebook source
// MAGIC %md
// MAGIC # Notebook de desarrollo de consultas Spark SQL
// MAGIC
// MAGIC #### Objetivo
// MAGIC Este notebook una herramienta generica poder desarrollar nuestra consulta SQL a partir de los datos de entrada
// MAGIC
// MAGIC
// MAGIC | Parámetro | Valor por defecto | Descripcion |
// MAGIC |---|---|---|
// MAGIC |sources||<table class="GeneratedTable">  <thead>    <tr>      <th>Parámetro</th>      <th>Descripción</th>    </tr>  </thead>  <tbody>    <tr>          </tr>    <tr>          </tr>    <tr>      <td>source_object</td>      <td>Ruta en la que se encuentran los datos</td>    </tr>    <tr>      <td>source_format</td>      <td>Formato de la ruta a leer</td>    </tr>  <tr>      <td>source_format_options</td>      <td>Opciones del formato</td>    </tr>       <tr>      <td>source_deep_partition</td>      <td>Carga la última partición de la profuncidad indicada (0 lo desactiva)</td>    </tr>     <tr>      <td>source_alias</td>      <td>Nombre para eferenciar este origen desde SQL</td>    </tr>           </tbody></table>|
// MAGIC |sql||Consulta que se ejecutará sobre los datos cargados|
// MAGIC
// MAGIC #### Nota
// MAGIC
// MAGIC Este notebook se puede usar para obtener los datos de salida necesarios para los tests

// COMMAND ----------

val sources="""[{
            "source_object" :"datahub01%env%aucdatagold.sandbox_crp0025.employee_details",
            "source_format": "table",
            "source_alias": "table_1",
            "source_deep_partition": 0,
            "source_alias": "table_1",
            "source_format_options": {}
            },
            {
            "source_object" :"datahub01%env%aucdatagold.sandbox_crp0025.department_info",
            "source_format": "table",
            "source_deep_partition": 0,
            "source_alias": "table_2"
           
            }]"""

// COMMAND ----------

// MAGIC %md
// MAGIC # Leer el dataset de origen 

// COMMAND ----------

val sql = """SELECT e.id, e.name, d.dept_name, e.year, e.month, e.day
FROM table_1 e
JOIN table_2 d
ON e.department = d.dept_name
order by year """

// COMMAND ----------

// MAGIC %md
// MAGIC ### Defining Helper Functions

// COMMAND ----------

// MAGIC %run /Shared/DAB-EYP0007/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_nb_arquetipo_functions

// COMMAND ----------

implicit val spark1:SparkSession=spark

// COMMAND ----------

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
mapper.registerModule(DefaultScalaModule)
mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
val sourcesFinal: List[Map[String, Any]] = mapper.readValue[List[Map[String, Any]]](sources)

// COMMAND ----------

// MAGIC %md
// MAGIC Read the source dataset

// COMMAND ----------

def getData = {
    sourcesFinal.foreach( x => getDataFromSource(x))
    spark.sql(sql)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ##Presentation of results

// COMMAND ----------

display(getData)

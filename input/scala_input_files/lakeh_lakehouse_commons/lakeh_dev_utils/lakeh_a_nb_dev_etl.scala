// Databricks notebook source
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

val sql = """SELECT e.id, e.name, d.dept_name, e.year, e.month, e.day
FROM table_1 e
JOIN table_2 d
ON e.department = d.dept_name
order by year """

// COMMAND ----------

// MAGIC %md
// MAGIC ### Defining Helper Functions

// COMMAND ----------

// MAGIC %run /Shared/DAB-PAN0001/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_a_nb_arquetipo_functions

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
// MAGIC Presentation of results

// COMMAND ----------

display(getData)

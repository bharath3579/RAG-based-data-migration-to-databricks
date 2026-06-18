// Databricks notebook source
// MAGIC %md
// MAGIC # Notebook para la consulta de datos contenidos en el DL
// MAGIC
// MAGIC Este notebook está pensado para obtener los datos necesarios para los tests

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

val sql = """SELECT e.id, e.name, d.dept_name, e.year, e.month, e.day
FROM table_1 e
JOIN table_2 d
ON e.department = d.dept_name
order by year """

// COMMAND ----------

// MAGIC %run /Shared/DAB-EYP0007/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_nb_arquetipo_functions

// COMMAND ----------

// MAGIC %md
// MAGIC # Instrucciones para la exprotación
// MAGIC ### CSV:
// MAGIC - La cabecera no se debe de eliminar
// MAGIC - Revisar que los campos de texto no contengan el delimitador, en caso contrario habrá que modificar el fichero para cambiar el delimitador
// MAGIC
// MAGIC ### JSON
// MAGIC - Se debe elimitar la estructrura _fields_
// MAGIC - Se debe quitar el campo _rows_ respetando su valor, una lista con todos los valores del dataset
// MAGIC - Se permiten JSON multilinea

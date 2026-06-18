// Databricks notebook source
val tableName=dbutils.widgets.get("tableName")
val newClusterKeys=dbutils.widgets.get("newClusterKeys")
val runFullOptimize=dbutils.widgets.get("runFullOptimize")

println(s"tableName $tableName , newClusterKeys $newClusterKeys ,runFullOptimize $runFullOptimize ")


// COMMAND ----------

// MAGIC %run /Shared/DAB-PAN0001/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_nb_arquetipo_functions

// COMMAND ----------

updateClusteringAndOptimize(
  tableName,
  newClusterKeys.split(",").toSeq,
  runFullOptimize.toBoolean
)


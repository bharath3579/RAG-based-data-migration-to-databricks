// Databricks notebook source
val tableOrSchemaName=dbutils.widgets.get("tableOrSchemaName")
val retentionDays=dbutils.widgets.get("retentionDays")
val updateRetentionProperty=dbutils.widgets.get("updateRetentionProperty")
val runForAllTablesInSchema=dbutils.widgets.get("runForAllTablesInSchema")

// COMMAND ----------

// MAGIC %run /Shared/DAB-PAN0001/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_nb_arquetipo_functions

// COMMAND ----------

runVacuumWithRetention(
  tableOrSchemaName,
   retentionDays.toInt,
  updateRetentionProperty.toBoolean,
  runForAllTablesInSchema.toBoolean
)


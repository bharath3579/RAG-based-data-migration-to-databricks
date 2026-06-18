// Databricks notebook source
// MAGIC %md
// MAGIC # Notebook de finalizacion de Registro de log

// COMMAND ----------

dbutils.widgets.text("digitalCaseCode", "PAN0001")
val digitalCaseCode: String = dbutils.widgets.get("digitalCaseCode")

dbutils.widgets.text("uniqueKey", "")
val uniqueKey: String = dbutils.widgets.get("uniqueKey")

dbutils.widgets.text("step", "0") 
val step = dbutils.widgets.get("step").toInt

dbutils.widgets.text("result", "")
val result: String = dbutils.widgets.get("result")

dbutils.widgets.text("msg", "")
val msg: String = dbutils.widgets.get("msg")

// COMMAND ----------

// DBTITLE 1,need to change this based upon the class changes for the linked service
import com.repsol.datalake.control._

ControlHelper().setCustomDigitalCase(digitalCaseCode)

ControlHelper().endControlTable(spark, uniqueKey, step, result, msg)

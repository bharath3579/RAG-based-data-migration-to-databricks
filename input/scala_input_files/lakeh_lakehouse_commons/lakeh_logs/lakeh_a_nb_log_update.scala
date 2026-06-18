// Databricks notebook source
// MAGIC %md
// MAGIC # Notebook de actualización de Registro de log

// COMMAND ----------

dbutils.widgets.text("digitalCaseCode", "")
val digitalCaseCode = dbutils.widgets.get("digitalCaseCode")

dbutils.widgets.text("uniqueKey", "")
val uniqueKey = dbutils.widgets.get("uniqueKey")

dbutils.widgets.text("step", "0") 
val step = dbutils.widgets.get("step").toInt

dbutils.widgets.text("msg", "")
val msg = dbutils.widgets.get("msg")

dbutils.widgets.text("extraData1", "")
val extraData1 = dbutils.widgets.get("extraData1")

dbutils.widgets.text("extraData2", "")
val extraData2 = dbutils.widgets.get("extraData2")

// COMMAND ----------

import com.repsol.datalake.control._

ControlHelper().setCustomDigitalCase(digitalCaseCode)

val eDat1 = if (extraData1.trim.isEmpty) None else Some(extraData1)
val eDat2 = if (extraData2.trim.isEmpty) None else Some(extraData2)

ControlHelper().updateControlTable(spark, uniqueKey, step, msg.trim, eDat1, eDat2)

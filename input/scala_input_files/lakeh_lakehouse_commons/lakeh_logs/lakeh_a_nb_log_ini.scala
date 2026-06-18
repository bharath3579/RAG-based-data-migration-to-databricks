// Databricks notebook source
// MAGIC %md
// MAGIC # Notebook de inicialización de Registro de log

// COMMAND ----------

dbutils.widgets.text("digitalCaseCode", "")
val digitalCaseCode: String = dbutils.widgets.get("digitalCaseCode")

dbutils.widgets.text("pipelineName", "")
val pipelineName: String = dbutils.widgets.get("pipelineName")

dbutils.widgets.text("parameters", "")
val parameters: String = dbutils.widgets.get("parameters")

dbutils.widgets.text("applicationName", "")
val applicationName: String = dbutils.widgets.get("applicationName")

dbutils.widgets.text("dataSetIn", "")
val dataSetIn: String = dbutils.widgets.get("dataSetIn")

dbutils.widgets.text("dataSetOut", "")
val dataSetOut: String = dbutils.widgets.get("dataSetOut")

dbutils.widgets.text("numSteps", "0") 
val numSteps: Int = dbutils.widgets.get("numSteps").toInt

dbutils.widgets.text("timeToLive", "60")
val timeToLive: Int = dbutils.widgets.get("timeToLive").toInt

// COMMAND ----------

// DBTITLE 1,need to change this based upon the class changes for the linked service
import com.repsol.datalake.control._

ControlHelper().setCustomDigitalCase(digitalCaseCode)

val uniqueKey = ControlHelper().initControlTable(spark, applicationName, pipelineName, parameters, numSteps, timeToLive=timeToLive, dataSetIN=dataSetIn, dataSetOUT=dataSetOut)

// COMMAND ----------

dbutils.notebook.exit(uniqueKey)

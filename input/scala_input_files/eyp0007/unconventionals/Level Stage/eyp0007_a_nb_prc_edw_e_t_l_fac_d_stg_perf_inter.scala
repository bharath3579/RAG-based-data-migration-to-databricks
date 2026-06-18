// Databricks notebook source
dbutils.widgets.text("applicationName", "")
val applicationName: String = dbutils.widgets.get("applicationName")
dbutils.widgets.text("uuid", "N/A")
val uuid: String = dbutils.widgets.get("uuid")
dbutils.widgets.text("parentUid", "N/A")
val puid: String = dbutils.widgets.get("parentUid")
dbutils.widgets.text("step", "0")
val step: Int = dbutils.widgets.get("step").toInt

dbutils.widgets.text("sources", "[{}]")
val sources: String = dbutils.widgets.get("sources")
dbutils.widgets.text("target", "{}")
val target: String = dbutils.widgets.get("target")

dbutils.widgets.text("name", "")
val name: String = dbutils.widgets.get("name")

dbutils.widgets.text("auxParams", "{}") 
val auxParams: String = dbutils.widgets.get("auxParams")
dbutils.widgets.text("params", "{}")
val params: String = dbutils.widgets.get("params")
dbutils.widgets.text("sparkProperties", """{}""")
val sparkProperties: String = dbutils.widgets.get("sparkProperties")
dbutils.widgets.text("referenceDate", "")
val referenceDate: String = dbutils.widgets.get("referenceDate")
dbutils.widgets.text("digitalCase", "")
val digitalCase: String = dbutils.widgets.get("digitalCase")
dbutils.widgets.text("uniqueKey", "")
val uniqueKey: String = dbutils.widgets.get("uniqueKey")
dbutils.widgets.text("optimizeDelta", "false")
val optimizeDelta: Boolean = dbutils.widgets.get("optimizeDelta").toBoolean
var error:String = ""

// COMMAND ----------

// MAGIC %run /Shared/DAB-EYP0007/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_a_nb_arquetipo_functions

// COMMAND ----------

// MAGIC %run /Shared/DAB-EYP0007/files/src/eyp0007/utilities/eyp0007_a_nb_gen_gen_functions

// COMMAND ----------

val sourcesFinal: Map[String, Map[String, Any]] = joinDatasets("",sources, "source")
val targetFinal: Map[String, Any] = parseMapAny(target)
val auxParamsFinal: Map[String, Any] = parseMapAny(auxParams)
val paramsFinal: Map[String, Any] = parseMapAny(if (params.trim.isEmpty) "{}" else params)
val sparkPropertiesFinal: Map[String, String] = parseMapString(sparkProperties)
 
// Set each Spark configuration property from the 'sparkPropertiesFinal' Map
sparkPropertiesFinal.foreach(item => {
    println(s"Setting spark property ${item._1} to ${item._2}")
    spark.conf.set(item._1, item._2)
})

implicit val myDigitalCase = digitalCase

val sqlFile = auxParamsFinal.getOrElse("sqlFile", "").toString.trim
val sqlquery = auxParamsFinal.getOrElse("sql", "").toString.trim

val init_sql =
  if (sqlFile.nonEmpty) {
    {readFile(s"config_$digitalCase/config_files/sql/$sqlFile")}
  } else if (sqlquery.nonEmpty) {
    sqlquery
  } else {
    println("Neither 'sqlFile' nor 'sql' parameter are not present in auxParams.")
    ""
  }

val sql = init_sql

// Extract target details from the parsed 'target' JSON
val target_object = targetFinal("target_object").toString
val target_operation_name = targetFinal("target_operation_name").toString

// COMMAND ----------



implicit val uid_app = (uuid, puid, applicationName)

if (digitalCase.trim.nonEmpty) {
    ControlHelper().setCustomDigitalCase(digitalCase)
}

LogHelper().logStart()

println(s"applicationName = $applicationName")
println(s"uuid = $uuid")
println(s"puid = $puid")
println(s"step = $step")
println(s"uniqueKey = $uniqueKey")
println(s"sources = $sources")
println(s"target = $target")
println(s"name = $name")

// COMMAND ----------

// MAGIC %md
// MAGIC # Código que implementa la lógica necesaria

// COMMAND ----------

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
val order = Seq("cod_vers_wellbore","num_stage","num_perforations_sum", "min_val_perf_top", "max_val_perf_base")


// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

def addNumPerforations(df: DataFrame): DataFrame = {

  // 1. Definir una ventana que agrupa por cod_vers_wellbore y num_stage
  val windowSpec = Window.partitionBy("cod_vers_wellbore", "num_stage")

  // 2. Calcular num_perforations y sumar sus valores por grupo
  val dfWithPerforations = df.withColumn(
      "num_perforations",
      (col("val_perf_base") - col("val_perf_top")) * col("num_shot_density")
    ).withColumn(
      "num_perforations_sum",
      sum(col("num_perforations")).over(windowSpec)
    )

  // 3. Eliminar duplicados para que quede solo una fila por combinación (cod_vers_wellbore, num_stage)
  dfWithPerforations
}



// COMMAND ----------

def agregarMinMax(df: DataFrame): DataFrame = {
  // Agrupamos por cod_vers_wellbore y num_stage, y calculamos el mínimo de val_perf_top y el máximo de val_perf_base
  val dfAgregado = df.groupBy("cod_vers_wellbore", "num_stage")
    .agg(
      min("val_perf_top").alias("min_val_perf_top"),  // Mínimo de val_perf_top
      max("val_perf_base").alias("max_val_perf_base") // Máximo de val_perf_base
    )

  // Unimos el DataFrame original con el DataFrame agregado para añadir las nuevas columnas
  val dfConMinMax = df.join(dfAgregado, Seq("cod_vers_wellbore", "num_stage"), "left")

  dfConMinMax
}

// COMMAND ----------


var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {   

        val dfCompPerfo = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_com_perf"))

        df = addNumPerforations(dfCompPerfo)
      
        df = agregarMinMax(df)

        df = df.select(order.map(col): _*)
        df= df.dropDuplicates("cod_vers_wellbore","num_stage")
              
        df = df.withColumn("num_perforations_sum", col("num_perforations_sum").cast("Int"))
        .withColumnRenamed("num_perforations_sum", "num_perforations")
        .withColumnRenamed("min_val_perf_top", "val_perf_top")
        .withColumnRenamed("max_val_perf_base", "val_perf_base")
        df = applyDDLFromTableToDF(df,getObject((target_object).toString))
    writeWrapper(sourceDF = df, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

    val countWrites = df.count()
  println(s"Process complete with: $countWrites written rows")

    dlInsert = "OK"

} catch {
    case e: Throwable => 
                dlMsg = e.toString
                dlInsert = "KO"
                LogHelper().logEndKO(dlMsg)
}


println(dlInsert)
println(dlMsg)

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC # Informar del resultado

// COMMAND ----------


val result = s"""{"error": "$error","dlInsert": { "result": "$dlInsert", "msg": "$dlMsg" }}"""

if (uniqueKey.nonEmpty && step > 0)
    ControlHelper().updateControlTable(spark, uniqueKey, step, s"$name: $result")

if (!error.isEmpty || dlInsert.equals("KO")) {
    LogHelper().logError(s"$name: $result")
    throw new Exception(s"$name: $result")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

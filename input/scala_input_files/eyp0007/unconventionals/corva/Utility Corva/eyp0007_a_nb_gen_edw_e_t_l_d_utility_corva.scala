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

println(s"applicationName = $applicationName")
println(s"uuid = $uuid")
println(s"puid = $puid")
println(s"uniqueKey = $uniqueKey")
println(s"sources = $sources")
println(s"target = $target")
println(s"auxParams = $auxParams")
println(s"params = $params")
println(s"name = $name")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val df1: DataFrame = getDataFromSource(sourcesFinal("table_1"))
val df2: DataFrame = getDataFromSource(sourcesFinal("table_2"))
def toScalaMap(obj: Any): Map[String, String] = obj match {
        case m: java.util.Map[_, _] => m.asScala.toMap.map { case (k, v) => k.toString -> v.toString }
        case m: Map[_, _] => m.map { case (k, v) => k.toString -> v.toString }
    }
val selectedDF = df2.select(
  col("cod_vers_wellbore").alias("vers_id"),
  col("cod_vers"),
  col("num_stage").alias("num_stage_completion"),
  col("cod_uwbi"),
  col("id_asset").alias("asset_id_completion"),
  col("fec_time_start"),
  col("cod_vers_well_string").alias("cod_vers_well_string_inicial"),
  col("cod_vers_well").alias("cod_vers_well_inicial")
)
val selectedDFc = selectedDF.withColumn("fec_time_start", unix_timestamp(col("fec_time_start")).cast("integer"))

var renamedDF = df1
val mapFields = auxParamsFinal.get("map_fields").map(toScalaMap).getOrElse(Map.empty[String, String])
mapFields.foreach { case (sourceField, targetField) =>
  renamedDF = renamedDF.withColumnRenamed(sourceField, targetField)
}
val windowasset = Window.partitionBy("num_stage", "asset_id").orderBy(desc("fec_time_start"))
val joinedDF = selectedDFc
  .join(
    renamedDF,
    selectedDFc("num_stage_completion") === renamedDF("num_stage") &&
    selectedDFc("asset_id_completion") === renamedDF("asset_id"),
    "right"
  ).withColumn("cod_vers_wellbore", coalesce(selectedDFc("vers_id"), lit("-99"))).withColumn("cod_vers_well_string", coalesce(selectedDFc("cod_vers_well_string_inicial"), lit("-99"))).withColumn("cod_vers_well", coalesce(selectedDFc("cod_vers_well_inicial"), lit("-99"))).withColumn("cod_vers", coalesce(selectedDFc("cod_vers"), lit("-99")))

val dfrow = joinedDF.withColumn("row_num", row_number().over(windowasset)).filter(col("row_num") === 1)
val finalDF = dfrow.drop("num_stage_completion","asset_id_completion","asset_id","vers_id","fec_time_start","row_num","cod_vers_well_string_inicial","cod_vers_well_inicial")



// COMMAND ----------

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

def autoConvertUnixToTimestamp(df: DataFrame): DataFrame = {
  // Obtener el esquema del DataFrame
  val schema = df.schema

  // Generar una lista de columnas, convirtiendo a Timestamp las columnas que son Integer
  val columns = schema.map { field =>
    field.dataType match {
      case IntegerType if field.name.startsWith("fec_") => // Solo columnas que comienzan con "fec_"
        to_timestamp(from_unixtime(col(field.name))).as(field.name)
      case _ => col(field.name) // Mantener las columnas sin cambios
    }
  }

  // Seleccionar las columnas transformadas
  df.select(columns: _*)
}

def castValElapsedTimeIfExists(df: DataFrame): DataFrame = {
  // Verificar si la columna "val_elapsed_time" existe en el DataFrame
  if (df.columns.contains("val_elapsed_time")) {
    // Si existe, castearla a Double
    df.withColumn("val_elapsed_time", col("val_elapsed_time").cast(DoubleType))
  } else {
    // Si no existe, retornar el DataFrame original
    df
  }
}

// COMMAND ----------

import org.apache.spark.sql.types.StringType
/**
 * Añade al DataFrame una columna "cod_uwbi_num_stage" con la concatenación
 * de cod_uwbi y num_stage (ambos como String).
 *
 * @param df            DataFrame de entrada.
 * @param codUwbiCol    Nombre de la columna cod_uwbi (por defecto "cod_uwbi").
 * @param numStageCol   Nombre de la columna num_stage (por defecto "num_stage").
 * @param newColName    Nombre de la nueva columna (por defecto "cod_uwbi_num_stage").
 * @return              DataFrame con la columna concatenada añadida.
 */
def addCodUwbiNumStage(
  df: DataFrame,
  codUwbiCol: String     = "cod_uwbi",
  numStageCol: String    = "num_stage",
  newColName: String     = "cod_uwbi_num_stage"
): DataFrame = {
  df.withColumn(
    newColName,
    concat(
        col(numStageCol).cast(StringType),
      col(codUwbiCol).cast(StringType)
    )
  )
}

// COMMAND ----------

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Column}

var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {   
        val dfCasteado= finalDF
        val dftdate = autoConvertUnixToTimestamp(dfCasteado)
        df = castValElapsedTimeIfExists(dftdate)

        df = addCodUwbiNumStage(df, "cod_uwbi", "num_stage", "cod_uwbi_num_stage")

   val dfFormatted = applyDDLFromTableToDF(df,getObject((target_object).toString))
   writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

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


val result = s"""{"error": "$error"}"""

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

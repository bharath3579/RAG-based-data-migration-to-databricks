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

import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



val orderFields: Seq[String] = Seq("cod_vers_wellbore","cod_vers_well_string", "cod_vers_well", "cod_uwbi", "val_station_md", "val_station_tvd", 
                                   "val_station_dev", "val_station_az", "val_station_az_grid", "val_station_x_coord", "val_station_y_coord", "num_is_certified")



def join (dfL: DataFrame, dfR: DataFrame, key: String): DataFrame = {

    val windowFilter = Window.partitionBy("cod_uwbi", "val_station_md").orderBy(desc("fec_start_date_rel"))

    var dfTrn: DataFrame = null

    dfTrn = dfL.join(dfR, Seq(key), "left").withColumn("cod_vers_wellbore", coalesce(dfR("id_vers_rel"), lit("-99"))).withColumn("cod_vers_well_string", coalesce(dfR("id_vers_well"), lit("-99"))).withColumn("cod_vers_well", coalesce(dfR("id_vers_well_hole"), lit("-99")))

    dfTrn = dfTrn.withColumn("row_number",row_number().over(windowFilter)).filter("row_number == 1")

return dfTrn
}

def transformationDimWellbore (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.select("fec_start_date_rel", "id_vers_rel", "cod_uwbi", "id_vers_well", "id_vers_well_hole")
                 .withColumn("cod_uwbi", trim(regexp_replace(col("cod_uwbi"), "\\s+", "")))

    return dfTrn
}



// COMMAND ----------

def unionDistinctWellbores(df1: DataFrame, df2: DataFrame): DataFrame = {
  // Seleccionar solo los WELLBORE_ID del primer DataFrame
  val wellboreIdsDF1 = df1.select("cod_uwi").distinct()

  // Filtrar los registros del segundo DataFrame cuyo WELLBORE_ID no esté en el primero
  val filteredDF2 = df2.join(wellboreIdsDF1, Seq("cod_uwi"), "left_anti")

  // Asegurarse de que los DataFrames tienen el mismo esquema
  val df2Aligned = filteredDF2.select(df1.columns.map(col): _*)

  // Hacer la unión de df1 y los registros únicos de df2
  val resultDF = df1.union(df2Aligned)

  resultDF
}

def replaceAzimuthnull(df: DataFrame): DataFrame = {

  df.withColumn("val_station_az_grid", when(col("val_station_az_grid").isNull, col("val_station_az")).otherwise(col("val_station_az_grid")))

}

// COMMAND ----------



var df: DataFrame  = null



try {

    val uniontrajectorysurv = unionDistinctWellbores(getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_fac_d_def_surv_stat")), getDataFromSource(sourcesFinal("am_eyp0007_tb_fac_d_trajectory")))
    val dfDevSurvey = uniontrajectorysurv.withColumnRenamed("x_interpolado", "val_station_x_coord")
                                                              .withColumnRenamed("y_interpolado", "val_station_y_coord")
                                                              .withColumn("cod_uwbi", trim(regexp_replace(col("cod_uwbi"), "\\s+", "")))

    val dfDimWellbore = transformationDimWellbore(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_wellbore_un")))

    df = dfDevSurvey
    df = join(df, dfDimWellbore, "cod_uwbi")

    df = df

    df = df.select(orderFields.map(col): _*)
    df = replaceAzimuthnull(df)
    val dfFormatted = applyDDLFromTableToDF(df,getObject((target_object).toString))
    writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

    val countWrites = df.count()
    println(s"Process complete with: $countWrites written rows")
} catch {
    case e: Throwable => 
                error = e.toString
                println(error)
                LogHelper().logEndKO(error)
}



// COMMAND ----------


val result = s"""{"error": "$error"}"""

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

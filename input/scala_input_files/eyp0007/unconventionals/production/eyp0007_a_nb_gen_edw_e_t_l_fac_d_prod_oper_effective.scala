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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

def calculateEffective(volumesTable: DataFrame): DataFrame = {
  // Paso 1: Rellenar valores nulos con 0 en las columnas relevantes
  val filledTable = volumesTable.na.fill(Map(
    "ind_alloc_cond_vol_stb" -> 0,
    "ind_alloc_gas_vol_mscf" -> 0,
    "ind_alloc_oil_vol_stb" -> 0,
    "ind_alloc_water_vol_bbl" -> 0
  ))

  // Paso 2: Normalización de las columnas por fec_production_day
  val normalizedCondVol = filledTable
    .filter(col("ind_alloc_cond_vol_stb") > 0)
    .groupBy("fec_production_day", "id_well", "cod_vers_wellbore") // Mantener cod_vers_wellbore
    .agg(sum("ind_alloc_cond_vol_stb").alias("val_ind_effective_cond_vol_stb"))

  val normalizedGasVol = filledTable
    .filter(col("ind_alloc_gas_vol_mscf") > 0)
    .groupBy("fec_production_day", "id_well", "cod_vers_wellbore") // Mantener cod_vers_wellbore
    .agg(sum("ind_alloc_gas_vol_mscf").alias("val_ind_effective_gas_vol_mscf"))

  val normalizedOilVol = filledTable
    .filter(col("ind_alloc_oil_vol_stb") > 0)
    .groupBy("fec_production_day", "id_well", "cod_vers_wellbore") // Mantener cod_vers_wellbore
    .agg(sum("ind_alloc_oil_vol_stb").alias("val_ind_effective_oil_vol_stb"))

  val normalizedWaterVol = filledTable
    .filter(col("ind_alloc_water_vol_bbl") > 0)
    .groupBy("fec_production_day", "id_well", "cod_vers_wellbore") // Mantener cod_vers_wellbore
    .agg(sum("ind_alloc_water_vol_bbl").alias("val_ind_effective_water_vol_bbl"))

  // Paso 3: Unir todas las normalizaciones en una sola tabla
  val combinedTable = normalizedCondVol
    .join(normalizedGasVol, Seq("fec_production_day", "id_well", "cod_vers_wellbore"), "outer")
    .join(normalizedOilVol, Seq("fec_production_day", "id_well", "cod_vers_wellbore"), "outer")
    .join(normalizedWaterVol, Seq("fec_production_day", "id_well", "cod_vers_wellbore"), "outer")

  // Paso 4: Filtrar días efectivos donde cualquier producción > 0
  val filteredTable = combinedTable.filter(
    (col("val_ind_effective_cond_vol_stb") > 0) ||
    (col("val_ind_effective_gas_vol_mscf") > 0) ||
    (col("val_ind_effective_oil_vol_stb") > 0) ||
    (col("val_ind_effective_water_vol_bbl") > 0)
  )

  // Paso 5: Crear índice incremental para los días efectivos
  val windowSpec = Window.partitionBy("id_well", "cod_vers_wellbore").orderBy("fec_production_day") // Mantener cod_vers_wellbore en la partición
  val withDayIndex = filteredTable
    .withColumn("num_days", row_number().over(windowSpec))

  // Retornar la tabla final asegurando que cod_vers_wellbore esté presente
  withDayIndex
}



// COMMAND ----------

import org.apache.spark.sql.types._



// COMMAND ----------

var dlInsert = "N/A"
var dlMsg = ""



var df: DataFrame  = null

try {

    val dfRaw = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_well_prod_oper"))

    val dfWellbore = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_wellbore_un"))

    val dfWellhole = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hole_unc"))
    
    val dfWellb = dfWellbore.orderBy(desc("cod_uwbi")).dropDuplicates("id_well")

    val dfWellboreSelected = dfWellb.select("id_vers_rel", "id_well")
    .withColumnRenamed("id_well", "id_well_wellbore")
    .withColumnRenamed("id_vers_rel", "cod_vers_wellbore")

    val dfJoined = dfRaw.join(
        dfWellboreSelected,
        dfRaw("id_well") === dfWellboreSelected("id_well_wellbore"),
        "left"
    ).withColumn("cod_vers_wellbore", coalesce(dfWellboreSelected("cod_vers_wellbore"), lit("-99"))).drop("id_well_wellbore")

    val dfEffect = calculateEffective(dfJoined)

    df = dfEffect.withColumnRenamed("id_well", "cod_well")

    // df = castDoubleToDoubleAndSmallDateTimeToDateTime(df)
    val dfFormatted = applyDDLFromTableToDF(df,getObject((target_object).toString))

    writeWrapper(sourceDF = df, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)
    
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

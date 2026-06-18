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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col

def transformEverus(df: DataFrame): DataFrame = {
    val selectedColumns = Seq(
        "API_UWI",
        "Unformatted_API_UWI",
        "API_UWI_12",
        "Unformatted_API_UWI_12",
        "API_UWI_14",
        "Unformatted_API_UWI_14",
        "CompletionID",
        "WellPadID",
        "WellPadDirection",
        "WellName",
        "Country",
        "StateProvince",
        "County",
        "LeaseName",
        "ENVOperator",
        "ENVTicker",
        "ENVBasin",
        "ENVPlay",
        "ENVInterval",
        "ENVSpacingAssumption",
        "ENVWellStatus",
        "Trajectory",
        "Formation",
        "FirstProdDate",
        "Latitude",
        "Longitude",
        "Latitude_BH",
        "Longitude_BH",
        "TVD_FT",
        "MD_FT",
        "WellNumber",
        "Vintage",
        "ElevationGL_FT",
        "SpudDate",
        "CompletionDate",
        "UpperPerf_FT",
        "LowerPerf_FT",
        "LateralLength_FT",
        "FracStages",
        "ProppantIntensity_LBSPerFT",
        "FluidIntensity_BBLPerFT",
        "First3MonthProd_BOE",
        "First3MonthProd_BOEPer1000FT",
        "First3MonthGas_MCF",
        "First3MonthGas_MCFPer1000FT",
        "First3MonthOil_BBL",
        "First3MonthOil_BBLPer1000FT",
        "First3MonthWater_BBL",
        "First6MonthProd_BOE",
        "First6MonthProd_BOEPer1000FT",
        "First6MonthGas_MCF",
        "First6MonthGas_MCFPer1000FT",
        "First6MonthOil_BBL",
        "First6MonthOil_BBLPer1000FT",
        "First6MonthWater_BBL",
        "First9MonthProd_BOE",
        "First9MonthProd_BOEPer1000FT",
        "First9MonthGas_MCF",
        "First9MonthGas_MCFPer1000FT",
        "First9MonthOil_BBL",
        "First9MonthOil_BBLPer1000FT",
        "First9MonthWater_BBL",
        "First12MonthProd_BOE",
        "First12MonthProd_BOEPer1000FT",
        "First12MonthGas_MCF",
        "First12MonthGas_MCFPer1000FT",
        "First12MonthOil_BBL",
        "First12MonthOil_BBLPer1000FT",
        "First12MonthWater_BBL",
        "First36MonthProd_BOE",
        "First36MonthProd_BOEPer1000FT",
        "First36MonthGas_MCF",
        "First36MonthGas_MCFPer1000FT",
        "First36MonthOil_BBL",
        "First36MonthOil_BBLPer1000FT",
        "First36MonthWater_BBL",
        "PeakProductionDate",
        "MonthsToPeakProduction",
        "PeakProd_BOE",
        "PeakProd_BOEPer1000FT",
        "PeakGas_MCF",
        "PeakGas_MCFPer1000FT",
        "PeakOil_BBL",
        "PeakOil_BBLPer1000FT",
        "PeakWater_BBL",
        "TotalProducingMonths",
        "GOR_ScfPerBbl",
        "ToeAngle_DEG",
        "ParentChildAnyZone",
        "ParentChildSameZone",
        "BoundedAnyZone",
        "BoundedSameZone",
        "DrillingCost_USDMM",
        "CompletionCost_USDMM",
        "TieInCost_USDMM",
        "TotalWellCost_USDMM",
        "EURWH_MBOE",
        "OilEURWH_MBBL",
        "GasEURWH_BCF",
        "GasInitialRate",
        "GasBFactor",
        "GasInitialDecline",
        "GasTerminalDecline",
        "OilInitialRate",
        "OilBFactor",
        "OilInitialDecline",
        "OilTerminalDecline",
        "ENVSpacingClass_AnyZone",
        "ENVSpacingClass_SameZone",
        "ENVSpacingClass_Within100",
        "ENVSpacingClass_AnyZoneAtInitialClass",
        "ENVSpacingClass_SameZoneAtInitialClass",
        "ENVSpacingClass_Vertical100AtInitialClass",
        "EURWH_MBOEPer1000FT",
        "OilEURWH_MBBLPer1000FT",
        "GasEURWH_BCFPer1000FT"
        )

        // Seleccionamos las columnas del DataFrame
        val dfSelected = df.select(selectedColumns.head, selectedColumns.tail: _*)
        dfSelected
}

def transformWellb(df: DataFrame): DataFrame = {

    val windowSpec = Window.partitionBy("cod_uwbi", "cod_api12").orderBy(col("fec_start_date_rel").desc)

    val dfwellbselect = df.select("id_vers_rel", "cod_uwbi", "cod_api12", "fec_start_date_rel")
    val dfRanked = dfwellbselect.withColumn("row_num", row_number().over(windowSpec))
    val dfresult = dfRanked.filter(col("row_num") === 1).drop("row_num")

    dfresult

}


def JoinEnverusWellb(df1: DataFrame, df2: DataFrame): DataFrame = {

    val dfJoin = df1.join(df2, df1("Unformatted_API_UWI_12") === df2("cod_api12"), "left")
    .withColumn("cod_vers_wellbore", coalesce(df2("id_vers_rel"), lit("-99"))).drop("fec_start_date_rel","id_vers_rel")
    dfJoin
    }




def renameNumericColumns(df: DataFrame): DataFrame = {
  // Obtener el esquema del DataFrame
  val schema = df.schema
  
  // Iterar sobre las columnas y renombrar según su tipo
  val renamedColumns = schema.map { field =>
    field.dataType match {
      case StringType if !field.name.startsWith("cod_") => col(field.name).as(s"des_${field.name}")
      case DoubleType  => col(field.name).as(s"val_${field.name}")
      case IntegerType => col(field.name).as(s"num_${field.name}")
      case _ => col(field.name)
    }
  }
  
  // Seleccionar las columnas con los nombres actualizados
  df.select(renamedColumns: _*)
}

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {   
        val dfenverus = transformEverus(getDataFromSource(sourcesFinal("`aa001065_usa_enverus-data`")))

        val dfwellb = transformWellb(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_wellbore_un")))

        df = JoinEnverusWellb(dfenverus,dfwellb)

        df = df
        df = renameNumericColumns(df)
        df.printSchema
        df = applyDDLFromTableToDF(df,getObject((target_object).toString))
    writeWrapper(sourceDF = df, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

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

// val sourceMap = Map(
//                         "source_object" -> s"${targetFinal("target_object")}",
//                         "source_format" -> "table"
//                     )
// val x= getDataFromSource(sourceMap)
// x.printSchema

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

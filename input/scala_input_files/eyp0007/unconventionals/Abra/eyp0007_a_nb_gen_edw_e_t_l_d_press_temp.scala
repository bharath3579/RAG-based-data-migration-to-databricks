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
val SelectedFinal = Seq("__calculated_key__", "cod_vers_wellbore", "cod_vers_well_string", "cod_vers_well", "cod_uwbi", "cod_job",
  "cod_api10", "des_wellbore_name", "des_source_port", "des_units_pressure", "des_units_temperature",
  "val_pressure", "val_temperature", "fec_utctime"
)

val SelectedGeneral = Seq("cod_job", "des_source_port", "cod_api10",
  "des_wellbore_name", "val_temperature", "val_pressure", "des_units_pressure", "des_units_temperature", "fec_utctime"
)

def transformationdf(df: DataFrame): DataFrame = {
  var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("jobId", "cod_job")
               .withColumnRenamed("flowrate", "val_flowrate")
               .withColumnRenamed("temperature", "val_temperature")
               .withColumnRenamed("pressure", "val_pressure")
               .withColumnRenamed("source_port", "des_source_port")
               .withColumnRenamed("Well_Government_identifier", "cod_api10")
               .withColumnRenamed("Wellbore_Name", "des_wellbore_name")
               .withColumnRenamed("units.pressure", "des_units_pressure")
               .withColumnRenamed("units.temperature", "des_units_temperature")
               .withColumn("val_pressure", col("val_pressure").cast("Double")) 
               .withColumn("val_temperature", col("val_temperature").cast("Double"))
  return dfTrn
}

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Encoders

def transformFullorInc (dffeed: DataFrame, dfjobinfo: DataFrame): DataFrame = {
    
    val df2jobinfo = dfjobinfo.withColumn("cod_api10", substring($"well_government_identifier", 1, 10))

    var dfTransactional: DataFrame  = null

    val flagFullLoad = paramsFinal.getOrElse("full_load", "false").toString.toBoolean

    if (flagFullLoad) {
        println("Carga Full")
        val dfhist: DataFrame = getDataFromSource(sourcesFinal("aa001028_usa_historical_data-data"))
        val df2feed = dffeed.withColumn("cod_api10", regexp_replace(col("wellid"), "-", "")).withColumn("fec_utctime", to_timestamp(col("utcTime"))).withColumnRenamed("jobId", "cod_job")
        .withColumnRenamed("wellName", "des_wellbore_name").withColumnRenamed("port", "des_source_port").withColumnRenamed("pressure", "val_pressure").withColumnRenamed("temperature", "val_temperature")
        .withColumnRenamed("units.pressure", "des_units_pressure").withColumnRenamed("units.temperature", "des_units_temperature")

        val df2feedandinfo = df2feed.select(SelectedGeneral.map(col): _*)

        val df2hist = dfhist.withColumnRenamed("source_id", "source_id_hist").withColumnRenamed("session_id", "cod_job").withColumn("fec_utctime", to_timestamp(from_unixtime($"epoch_date_time" / 1000)))
        .withColumn("des_units_temperature", lit(null).cast("string")).withColumn("des_units_pressure", lit(null).cast("string"))
        val dfhistandinfo = df2hist.join(df2jobinfo, df2hist("source_id_hist") === df2jobinfo("source_id") && df2hist("cod_job") === df2jobinfo("session_id"), "left")

        val df2histandinfo = dfhistandinfo.withColumn("cod_api10", substring($"well_government_identifier", 1, 10)).withColumnRenamed("wellbore_name", "des_wellbore_name").withColumnRenamed("source_port", "des_source_port")
        .withColumnRenamed("temperature", "val_temperature").withColumnRenamed("pressure", "val_pressure")

        val df3histandinfo = df2histandinfo.select(SelectedGeneral.map(col): _*)

        dfTransactional = df2feedandinfo.unionByName(df3histandinfo)
        val totalRows = dfTransactional.count()
        println(s"Total de filas: $totalRows")
       // dfTransactional = df3histandinfo.withColumn("__calculated_key__", concat(col("session_id"), col("source_id"),  col("ruwbi"), col("fec_utctime").cast("string")))


    }else {
        println("Carga incremental")

            val dffeeddate = dffeed.withColumn("fec_utctime", to_timestamp(col("utcTime")))

            val fieldsFilter = auxParamsFinal.getOrElse("fieldsFilter", List("fec_utctime")).asInstanceOf[List[String]]
            var referenceDateAux = if (referenceDate!= null) {referenceDate} else {""}
            if (referenceDateAux.isEmpty){
                referenceDateAux = returnDateNowString()
            }
            val filterWhere = getWhereFilterDays((referenceDateAux), fieldsFilter, 5)
            val dffeedfilter = dffeeddate.filter(filterWhere)

            val df2feed = dffeedfilter.withColumn("cod_api10", regexp_replace(col("wellid"), "-", "")).withColumnRenamed("jobId", "cod_job")
            .withColumnRenamed("wellName", "des_wellbore_name").withColumnRenamed("port", "des_source_port").withColumnRenamed("pressure", "val_pressure").withColumnRenamed("temperature", "val_temperature")
            .withColumnRenamed("units.pressure", "des_units_pressure").withColumnRenamed("units.temperature", "des_units_temperature")
            dfTransactional = df2feed.select(SelectedGeneral.map(col): _*)
            
            //dfTransactional = df2feedandinfo.withColumn("__calculated_key__", concat(col("session_id"), col("source_id"),  col("ruwbi"), col("fec_utctime").cast("string")))

        }

return dfTransactional

    
}

// COMMAND ----------


def Joinwellbdf (df: DataFrame, dfwellb: DataFrame): DataFrame = {

    val dfwellb2 = dfwellb.withColumn("cod_api12", trim(regexp_replace(col("cod_api12"), "\\s+", "")))

    val dfwellb3 = dfwellb2.withColumn("cod_api", substring(col("cod_api12"), 1, 10))

    val dfwellbore = dfwellb3.select("cod_uwbi","id_vers_rel", "id_vers_well", "id_vers_well_hole", "fec_start_date_rel", "cod_api")

    val dfwelljoin = df.join(dfwellbore, df("cod_api10") === dfwellbore("cod_api"), "left").withColumn("cod_vers_wellbore", coalesce(dfwellbore("id_vers_rel"), lit("-99"))).withColumn("cod_vers_well_string", coalesce(dfwellbore("id_vers_well"), lit("-99"))).withColumn("cod_vers_well", coalesce(dfwellbore("id_vers_well_hole"), lit("-99")))
    
    val window = Window.partitionBy("cod_uwbi","cod_api10", "des_source_port", "cod_job", "fec_utctime").orderBy(desc("fec_start_date_rel"))
    val dfjoin = dfwelljoin.withColumn("row_num", row_number().over(window)).filter(col("row_num") === 1)
    dfjoin
}

// COMMAND ----------

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Column}

var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {   
    
    val dfFullorInc = transformFullorInc(getDataFromSource(sourcesFinal("aa001028_usa_mqtt_feed")), getDataFromSource(sourcesFinal("aa001028_usa_job_info")))

    val dfJoindf = Joinwellbdf(dfFullorInc, getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_wellbore_un")))

    val countit = dfJoindf.count()
   
    println(s"Process complete with: $countit written rows")

    val dftransform = transformationdf(dfJoindf)

    val dfkeycalculated = dftransform.withColumn("__calculated_key__", concat(col("cod_job"), col("cod_api10"),col("des_source_port"), date_format(col("fec_utctime").cast("timestamp"), "yyyyMMddHHmmss")))

    val filteredDF = dfkeycalculated.filter($"__calculated_key__".isNotNull)
    val deduplicatedDF = filteredDF.dropDuplicates("__calculated_key__")

    df = deduplicatedDF.select(SelectedFinal.map(col): _*)
    df = df.filter(col("fec_utctime").isNotNull)

    val (targetDf, targetDelta) = getDeltaTable(df, targetFinal)
    applyChange(targetFinal, df)
 
    optimizeDeltaFun(targetDelta, getObject(targetFinal("target_object").toString) ,auxParamsFinal("dayOfWeek").toString)

    dlInsert = "OK"
    
    val countWrites = df.count()
   
    println(s"Process complete with: $countWrites written rows")

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

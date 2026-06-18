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

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.types._



val orderFields: Seq[String] = Seq("cod_vers_wellbore", "cod_vers_well_string", "cod_vers_well", "cod_uwbi", "cod_uwbi_num_stage", "num_stage" ,"val_perf_top", "val_perf_base","val_perf_orientation", "val_diameter", "val_charge_weight", 
                                    "des_charge", "num_shot_density", "val_carrier_weight", "des_carrier", "fec_interval_shot")


def transformationCompPerf (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("MD_TOP_SHOT", "val_perf_top")
                 .withColumnRenamed("MD_BOTTOM_SHOT", "val_perf_base")
                 .withColumnRenamed("CHARGE_PHASING", "val_perf_orientation")
                 .withColumnRenamed("GUN_DIAM_MAX", "val_diameter")
                 .withColumnRenamed("CHARGE_SIZE", "val_charge_weight")
                 .withColumnRenamed("SHOT_DENSITY", "num_shot_density").withColumn("num_shot_density", col("num_shot_density").cast("int"))
                 .withColumnRenamed("CHARGE_DESCRIPTION", "des_charge")
                 .withColumnRenamed("CARRIER_DESCRIPTION", "des_carrier")
                 .withColumnRenamed("CARRIER_SIZE", "val_carrier_weight")
                 .withColumnRenamed("DATE_INTERVAL_SHOT", "fec_interval_shot")
                 .withColumn("val_perf_mid", col("val_perf_top") + ((col("val_perf_base") - col("val_perf_top"))/2))
                 .select("WELLBORE_ID", "val_perf_top", "val_perf_base", "val_perf_orientation", "val_diameter", "val_charge_weight", "num_shot_density", "des_charge",
                          "des_carrier",  "val_carrier_weight",  "fec_interval_shot", "val_perf_mid")

return dfTrn
}

def transformationStimJob (df: DataFrame): DataFrame = {
    var dfTrn: DataFrame = df

    dfTrn = dfTrn.select("WELLBORE_ID", "STIM_JOB_ID").dropDuplicates

return dfTrn
}



def transformationWellboreEDM (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("WELLBORE_UWI", "cod_uwbi").select("cod_uwbi", "WELLBORE_ID", "WELL_ID","UPDATE_DATE").withColumn("cod_uwbi", trim(regexp_replace(col("cod_uwbi"), "\\s+", "")))

    return dfTrn
} 


def transformationWellEDM (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df
    dfTrn = dfTrn.withColumnRenamed("REPSOL_WELL_UWI", "cod_uwi")
                .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
                .select("cod_uwi", "WELL_ID")
                .dropDuplicates("cod_uwi")

    return dfTrn
} 

def transformationDimWell (df: DataFrame): DataFrame = {
        var dfTrn: DataFrame = df
        dfTrn = dfTrn.filter(col("des_state_or_province").isin("PENNSYLVANIA", "TEXAS", "NEW YORK"))
                     .select("cod_uwi","val_datum_elevation")
                     .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
                     .dropDuplicates
                    

    return dfTrn

} 

def transformationDimWellbore (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.select("fec_start_date_rel", "id_vers_rel", "cod_uwbi", "id_vers_well", "id_vers_well_hole").withColumnRenamed("cod_uwbi", "cod_uwbi_dim").withColumn("cod_uwbi_dim", trim(regexp_replace(col("cod_uwbi_dim"), "\\s+", "")))

    return dfTrn
}

def join (dfL: DataFrame, dfR: DataFrame, key: String): DataFrame = {

    var dfTrn: DataFrame = null
    dfTrn = dfL.join(dfR, Seq(key), "inner")

return dfTrn
}


def joinWellbore (dfL: DataFrame, dfR: DataFrame): DataFrame = {

    val  windowFilter = Window.partitionBy("WELLBORE_ID", "val_perf_mid").orderBy(desc("fec_start_date_rel"))

    var dfTrn: DataFrame = null

    dfTrn = dfL.join(dfR, dfL("cod_uwbi") === dfR("cod_uwbi_dim"), "left").withColumn("cod_vers_wellbore", coalesce(dfR("id_vers_rel"), lit("-99"))).withColumn("cod_vers_well_string", coalesce(dfR("id_vers_well"), lit("-99"))).withColumn("cod_vers_well", coalesce(dfR("id_vers_well_hole"), lit("-99")))
    
    val windowDF = dfTrn.withColumn("row_number",row_number().over(windowFilter)).filter("row_number == 1")

    return windowDF
}

def transformationCompStg (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("REPSOL_STAGE_NO", "num_stage")
                .withColumnRenamed("interval_top", "val_interval_top")
                .withColumnRenamed("interval_base", "val_interval_base")
                .withColumnRenamed("STIM_JOB_ID", "STIM_JOB_ID_join")
                .select("STIM_JOB_ID_join","num_stage", "val_interval_top", "val_interval_base")

    return dfTrn


}


def joinCompStage(dfL: DataFrame, dfR: DataFrame): DataFrame = {

    var dfTrn: DataFrame = null

    dfTrn = dfL.join(dfR, dfL("STIM_JOB_ID") === dfR("STIM_JOB_ID_join") && dfL("val_perf_mid") > dfR("val_interval_top") && dfL("val_perf_mid") < dfR("val_interval_base"), "left")

    dfTrn = dfTrn.dropDuplicates("WELLBORE_ID", "val_perf_mid")


    return dfTrn

}


// COMMAND ----------

def updatePerforation(df:DataFrame): DataFrame = {
  df.withColumn("val_perf_top", col("val_perf_top") + col("val_datum_elevation")).withColumn("val_perf_base", col("val_perf_base") + col("val_datum_elevation"))
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

var dlInsert = "N/A"
var dlMsg = ""




var df: DataFrame  = null

try {
    
    val dfCompPerf = transformationCompPerf(getDataFromSource(sourcesFinal("aa001236_glb_cd_perf_interval_t")))
    
    val dfWellboreEdm = transformationWellboreEDM(getDataFromSource(sourcesFinal("aa001236_glb_cd_wellbore_t")))
    
    val dfWellEdm = transformationWellEDM(getDataFromSource(sourcesFinal("aa001236_glb_cd_well_t")))
    
    val dfDimWell = transformationDimWell(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hole_unc")))
    
    val dfStimJ = transformationStimJob(getDataFromSource(sourcesFinal("aa001236_glb_dm_stim_job_t")))
    dfStimJ.printSchema

    val dfCompStg = transformationCompStg(getDataFromSource(sourcesFinal("aa001236_glb_dm_stim_treatment_t")))

    val dfDimWellbore = transformationDimWellbore(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_wellbore_un")))

    df = dfCompPerf

    df = join(df, dfWellboreEdm, "WELLBORE_ID")

    df = join(df, dfWellEdm, "WELL_ID")

    df = join(df, dfDimWell, "cod_uwi")
    
    df = join(df, dfStimJ, "WELLBORE_ID")
    
    df = joinCompStage(df, dfCompStg)

    df = joinWellbore(df, dfDimWellbore)

    df = df

    df = updatePerforation(df)

    val dataframesemiFinal = addCodUwbiNumStage(df, "cod_uwbi", "num_stage", "cod_uwbi_num_stage")

    df = dataframesemiFinal.select(orderFields.map(col): _*)
    df = applyDDLFromTableToDF(df,getObject((target_object).toString))
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

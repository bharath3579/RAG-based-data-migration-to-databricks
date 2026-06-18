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

// MAGIC %md
// MAGIC # Código que implementa la lógica necesaria

// COMMAND ----------

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.types._




val orderFields: Seq[String] =  Seq("cod_vers_wellbore","cod_vers_well_string", "cod_vers_well", "cod_uwbi", "cod_uwbi_num_stage", "num_stage", "val_interval_top", "val_interval_base", "fec_time_start", "fec_time_end", "val_pump_duration", 
                         "val_prefrac_static_wh_press", "val_prefrac_isip", "val_postfrac_isip", "val_5min_isip", "avg_treating_pressure", "max_treating_pressure", 
                         "val_breakdown_pressure", "avg_pump_rate", "max_pump_rate", "val_stage_acid_volume", "val_stage_slurry_volume", "val_stage_clean_fluid", 
                         "val_stage_100mesh_sand_volume", "val_stage_40_70_sand_volume", "val_stage_30_50_sand_volume", "val_stage_20_40_sand_volume", "val_total_proppant", 
                         "val_fresh_water", "val_flowback_water", "val_stage_mid_perf_depth", "val_stage_mid_perf_depth_tvd", "val_stage_mid_perf_depth_abs_tvd", "val_stage_mid_perf_dev",
                         "val_stage_mid_perf_az", "val_stage_mid_perf_x_coord", "val_stage_mid_perf_y_coord", "val_time_between_stg", "val_prefrac_stc_wh_press_grad", "val_prefrac_isip_grad", "val_postfrac_isip_grad",
                         "val_press_decline_btw_stg","val_bh_press_aft_perf", "val_press_decl_5_min_shut_in", "val_rt_press_decline_btw_stg","val_rt_press_decl_5_m_shut_in", "val_stg_frac_flowback_water", "val_latitude", 
                         "val_longitude", "val_completed_length")



//Logica para proppants
val proppantMap: Map[List[String], String] = Map(List("10", "100") -> "val_stage_100mesh_sand_volume",
                                           List("4070") -> "val_stage_40_70_sand_volume", 
                                           List("3050") -> "val_stage_30_50_sand_volume", 
                                           List("2040") -> "val_stage_20_40_sand_volume")

def mapValue(value: String): String = {
proppantMap.collectFirst 
    {
case (keyList, mappedValue) if keyList.contains(value) => mappedValue
    }.getOrElse(null) // Si no coincide, devuelve el valor original
}

val mapUDF = udf((value: String) => mapValue(value))

def transformationStimProppant (df: DataFrame): DataFrame = {
    
    

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumn("proppant_size_cleaned", trim(lower(regexp_replace(col("proppant_size"), "[^0-9]", ""))))
                 .withColumn("mapped_value", mapUDF(col("proppant_size_cleaned")))
                 .filter(col("mapped_value").isNotNull)
                 .groupBy("STIM_TREATMENT_ID").pivot("mapped_value").agg(first("PROPPANT_USED"))

return dfTrn

}

//Logica para fluids

val fluidMap: Map[List[String], String] = Map(List("fesh", "fesh water", "fr", "frash water", "freesh water", "freh water", "frersh water", "fres water", "fresg water",
                                                "fresh", "fresh  water", "fresh warter", "fresh wateer", "fresh water", "fresh water with biocide and clay stabilizer",
                                                "fresh water- bbls pumpdown flush", "fresh watre", "fresh watrer","freshwater", "frresh water", "fw") -> "val_fresh_water",
                                             List("fb", "fkiwback", "fliwback", "floaback", "floiwback", "flow  back", "flow baack", "flow bac k", "flow bacck",
                                                "flow back", "flow bsck", "flowbacck", "flowback", "flowback water", "flowback water.",
                                                "flowbck", "flown back") -> "val_flowback_water")

def mapValueFluid(value: String): String = {
fluidMap.collectFirst 
    {
case (keyList, mappedValue) if keyList.contains(value) => mappedValue
    }.getOrElse(null) // Si no coincide, devuelve el valor original
}

val mapFluidUDF = udf((value: String) => mapValueFluid(value))

def transformationStimFluid(df: DataFrame): DataFrame = {
  
  var dfTrn: DataFrame = df
  
  dfTrn = dfTrn.withColumn("fluid_cleaned", 
                             trim(lower(regexp_replace(regexp_replace(col("fluid_comments"), "\\d+", ""), "[\\p{C}\\s]+$", ""))))
                 .withColumn("mapped_value", mapFluidUDF(col("fluid_cleaned")))
                 .filter(col("mapped_value").isNotNull)
                 .groupBy("STIM_TREATMENT_ID")
                 .pivot("mapped_value")
                 .agg(first("TOTAL_FLUID_PUMPED"))

  dfTrn
}


def transformationCompStageFinal (df : DataFrame): DataFrame = {

    val stimWindow = Window.partitionBy("STIM_JOB_ID").orderBy("num_stage")

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumn("val_stg_frac_flowback_water", col("val_flowback_water") / col("val_stage_clean_fluid"))
                 .withColumn("val_prefrac_isip_grad", (col("val_prefrac_isip") / col("val_stage_mid_perf_depth_tvd")) + 0.433)
                 .withColumn("val_postfrac_isip_grad", (col("val_postfrac_isip") / col("val_stage_mid_perf_depth_tvd")) + 0.433)
                 .withColumn("val_prefrac_stc_wh_press_grad", (col("val_prefrac_static_wh_press") / col("val_stage_mid_perf_depth_tvd")) + 0.433)
                 .withColumn("val_bh_press_aft_perf", (col("val_prefrac_isip") / col("val_stage_mid_perf_depth_tvd")) + 0.433)

return dfTrn
}


def joinStim(dfL: DataFrame, dfR: DataFrame, key: String): DataFrame = {
    
    var dfTrn: DataFrame = null

    dfTrn = dfL.join(dfR, Seq(key), "left")

    return dfTrn


}

def transformationDimWellbore (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.select("fec_start_date_rel", "id_vers_rel", "cod_uwbi", "id_vers_well", "id_vers_well_hole")
                 .withColumn("cod_uwbi", trim(regexp_replace(col("cod_uwbi"), "\\s+", "")))
                
    return dfTrn
}

def joinWellbore (dfL: DataFrame, dfR: DataFrame): DataFrame = {

    val  windowFilter = Window.partitionBy("cod_uwbi", "num_stage", "fec_time_start").orderBy(desc("fec_start_date_rel"))

    var dfTrn: DataFrame = null

    dfTrn = dfL.join(dfR, Seq("cod_uwbi"), "left").withColumn("cod_vers_wellbore", coalesce(dfR("id_vers_rel"), lit("-99"))).withColumn("cod_vers_well_string", coalesce(dfR("id_vers_well"), lit("-99"))).withColumn("cod_vers_well", coalesce(dfR("id_vers_well_hole"), lit("-99")))
    
    val windowDF = dfTrn.withColumn("row_number",row_number().over(windowFilter)).filter("row_number == 1")

    return windowDF
}



// COMMAND ----------

def convertToPounds(df: DataFrame): DataFrame = {
  val columnName = "val_total_proppant" // Nombre de la columna definido dentro de la función
  val conversionFactor = 2000
  df.withColumn(columnName, (col(columnName) * conversionFactor).cast("Double"))
}

// COMMAND ----------

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
    
    val dfStimT = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_fac_d_comp_stg_final_inter"))

    val dfStimP = transformationStimProppant(getDataFromSource(sourcesFinal("aa001236_glb_dm_stim_treatment_proppant_t")))

    val dfStimF = transformationStimFluid(getDataFromSource(sourcesFinal("aa001236_glb_dm_stim_treatment_fluid_t")))

    val dfWellbore = transformationDimWellbore(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_wellbore_un")))

    df = dfStimT

    df = joinStim(df, dfStimP, "STIM_TREATMENT_ID")

    df = joinStim(df, dfStimF, "STIM_TREATMENT_ID")
    
    df = df.withColumn("cod_uwbi", trim(regexp_replace(col("cod_uwbi"), "\\s+", "")))

    df = joinWellbore(df, dfWellbore)

    df = transformationCompStageFinal(df)
    
    df= convertToPounds(df)

    df = df
    val countWrites2 = df.count()
   
    println(s"Process complete with: $countWrites2 written rows")

    df = addCodUwbiNumStage(df, "cod_uwbi", "num_stage", "cod_uwbi_num_stage")
    display(df)
    df = df.select(orderFields.map(col): _*)
    display(df)

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

// MAGIC %md
// MAGIC # Informar del resultado

// COMMAND ----------


val result = s"""{"error": "$error"}"""

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

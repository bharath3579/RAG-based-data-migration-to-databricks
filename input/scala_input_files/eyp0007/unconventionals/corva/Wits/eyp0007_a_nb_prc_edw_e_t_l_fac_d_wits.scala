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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame


val order = Seq("__calculated_key__","cod_vers_wellbore", "cod_vers_well_string", "cod_vers_well", "cod_uwbi", "cod_wits", "fec_date", "num_stage", "val_elapsed_time", "val_wellhead_press", "val_tot_proppant_conc", 
"val_tot_proppant_mass", "val_clean_flow_rate", "val_slurry_flow_rate", "val_total_clean_vol", "val_total_slurry_vol", "val_friction_red_vol","val_pump_elapsed_time"
)

// Renombramientos para dfFinal
def transformationdf(df: DataFrame): DataFrame = {
  var dfTrn: DataFrame = df

    dfTrn = dfTrn//.withColumnRenamed("cod_vers_wellbore", "id_vers_wellbore")
               .withColumnRenamed("cod_uwbi", "cod_uwbi")
               .withColumnRenamed("datetime", "fec_date")
               .withColumnRenamed("stage_number", "num_stage")
               .withColumnRenamed("data.elapsed_time", "val_elapsed_time")
               .withColumnRenamed("data.wellhead_pressure", "val_wellhead_press")
               .withColumnRenamed("data.total_proppant_concentration", "val_tot_proppant_conc")
               .withColumnRenamed("data.total_proppant_mass", "val_tot_proppant_mass")
               .withColumnRenamed("data.clean_flow_rate_in", "val_clean_flow_rate")
               .withColumnRenamed("data.slurry_flow_rate_in", "val_slurry_flow_rate")
               .withColumnRenamed("data.total_clean_volume_in", "val_total_clean_vol")
               .withColumnRenamed("data.total_slurry_volume_in", "val_total_slurry_vol")
               .withColumnRenamed("data.total_friction_reducer", "val_friction_red_vol")
  return dfTrn
}


// COMMAND ----------

def transformFullorInc (dfwits: DataFrame): DataFrame = {

    var dfUnconventional: DataFrame  = null

    val flagFullLoad = paramsFinal.getOrElse("full_load", "false").toString.toBoolean

    if (flagFullLoad) {
        println("Carga Full")
        dfUnconventional = dfwits.withColumn("datetime", from_unixtime(col("timestamp")))
        //dfUnconventional = dfUnconventional.withColumn("__calculated_key__", col("_id"))
        
    }else {
        println("Carga incremental")

        val dfwits2 = dfwits.withColumn("datetime", from_unixtime(col("timestamp")))
        val fieldsFilter = auxParamsFinal.getOrElse("fieldsFilter", List("datetime")).asInstanceOf[List[String]]
        var referenceDateAux = if (referenceDate!= null) {referenceDate} else {""}
        if (referenceDateAux.isEmpty){
            referenceDateAux = returnDateNowString()
        }
        val filterWhere = getWhereFilterDays((referenceDateAux), fieldsFilter, 9)
        dfUnconventional = dfwits2.filter(filterWhere)
        //dfUnconventional = dfUnconventional.withColumn("__calculated_key__", col("_id"))
        }

return dfUnconventional

    
}

// COMMAND ----------



def Resample(df: DataFrame): DataFrame = {

    // Eliminamos las columnas que no nos interesan
    val dfWits = df.drop("version", "provider", "collection", "company_id", "data.timestamp", "data.is_valid", "timestamp")

    // Creamos una nueva columna para el "grupo" de 5 registros (por cada stage_number y asset_id)
    val dfWithGroup = dfWits
      .withColumn("group", (floor((row_number().over(Window.partitionBy("stage_number", "asset_id").orderBy("datetime")) - 1) / 5)).cast("int"))


    // Realizamos el resampleo agrupado por "stage_number", "asset_id" y "group"
    val dfResampled = dfWithGroup
      .groupBy("stage_number", "asset_id", "group")
      .agg(
        // Recolectamos los _id en una lista y luego seleccionamos el _id central
        collect_list("datetime").alias("datetime_list"),
        collect_list("_id").alias("ids"),
        
        // Calculamos los promedios de cada columna numérica, especificando cada columna individualmente con alias
        avg(col("`data.wellhead_pressure`")).alias("avg_wellhead_pressure"),
        avg(col("`data.slurry_flow_rate_in`")).alias("avg_slurry_flow_rate_in"),
        avg(col("`data.elapsed_time`")).alias("avg_elapsed_time"),
        avg(col("`data.clean_flow_rate_in`")).alias("avg_clean_flow_rate_in"),
        avg(col("`data.total_clean_volume_in`")).alias("avg_total_clean_volume_in"),
        avg(col("`data.total_slurry_volume_in`")).alias("avg_total_slurry_volume_in"),
        avg(col("`data.total_chemical_rate_in`")).alias("avg_total_chemical_rate_in"),
        avg(col("`data.total_friction_reducer`")).alias("avg_total_friction_reducer"),
        avg(col("`data.total_proppant_concentration`")).alias("avg_total_proppant_concentration"),
        avg(col("`data.total_proppant_mass`")).alias("avg_total_proppant_mass"),
        avg(col("`data.bottomhole_proppant_concentration`")).alias("avg_bottomhole_proppant_concentration"),
        avg(col("`data.hydrostatic_pressure`")).alias("avg_hydrostatic_pressure"),
        avg(col("`data.inverse_hydrostatic_pressure`")).alias("avg_inverse_hydrostatic_pressure"),
        avg(col("`data.enzyme_breaker`")).alias("avg_enzyme_breaker"),
        avg(col("`data.scale_inhibitor`")).alias("avg_scale_inhibitor"),
        avg(col("`data.surfactant`")).alias("avg_surfactant"),
        avg(col("`data.friction_reducer`")).alias("avg_friction_reducer"),
        avg(col("`data.friction_reducer_extra`")).alias("avg_friction_reducer_extra"),
        avg(col("`data.cross_linker`")).alias("avg_cross_linker"),
        avg(col("`data.acid`")).alias("avg_acid"),
        avg(col("`data.gel`")).alias("avg_gel"),
        avg(col("`data.ph_adjusting_agent`")).alias("avg_ph_adjusting_agent"),
        avg(col("`data.accelerator`")).alias("avg_accelerator"),
        avg(col("`data.fluid_loss`")).alias("avg_fluid_loss"),
        avg(col("`data.ploymer_plug`")).alias("avg_ploymer_plug"),
        avg(col("`data.acid_inhibitor`")).alias("avg_acid_inhibitor"),
        avg(col("`data.acid_retarder`")).alias("avg_acid_retarder"),
        avg(col("`data.emulsifier`")).alias("avg_emulsifier"),
        avg(col("`data.clay_stabilizer`")).alias("avg_clay_stabilizer"),
        avg(col("`data.non_emulsifier`")).alias("avg_non_emulsifier"),
        avg(col("`data.fines_suspender`")).alias("avg_fines_suspender"),
        avg(col("`data.anti_sludge`")).alias("avg_anti_sludge"),
        avg(col("`data.iron_control`")).alias("avg_iron_control"),
        avg(col("`data.oxygen_scavenger`")).alias("avg_oxygen_scavenger"),
        avg(col("`data.mutual_solvent`")).alias("avg_mutual_solvent"),
        avg(col("`data.corrosion_inhibitor`")).alias("avg_corrosion_inhibitor"),
        avg(col("`data.paraffin_control`")).alias("avg_paraffin_control"),
        avg(col("`data.biocide`")).alias("avg_biocide"),
        avg(col("`data.instant_crosslinker`")).alias("avg_instant_crosslinker"),
        avg(col("`data.delayed_crosslinker`")).alias("avg_delayed_crosslinker"),
        avg(col("`data.liquid_breaker`")).alias("avg_liquid_breaker"),
        avg(col("`data.powder_breaker`")).alias("avg_powder_breaker"),
        avg(col("`data.divertor`")).alias("avg_divertor"),
        avg(col("`data.powder_gel`")).alias("avg_powder_gel"),
        avg(col("`data.powder_friction_reducer`")).alias("avg_powder_friction_reducer"),
        avg(col("`data.powder_enzyme_breaker`")).alias("avg_powder_enzyme_breaker"),
        avg(col("`data.proppant_1_concentration`")).alias("avg_proppant_1_concentration"),
        avg(col("`data.proppant_2_concentration`")).alias("avg_proppant_2_concentration"),
        avg(col("`data.proppant_1_mass`")).alias("avg_proppant_1_mass"),
        avg(col("`data.delayed_crosslink`")).alias("avg_delayed_crosslink"),
        avg(col("`data.pumpside_pressure`")).alias("avg_pumpside_pressure"),
        avg(col("`data.offset_pressure_3h_surf`")).alias("avg_offset_pressure_3h_surf"),
        avg(col("`data.offset_pressure_4h_surf`")).alias("avg_offset_pressure_4h_surf"),
        avg(col("`data.offset_pressure_5h_surf`")).alias("avg_offset_pressure_5h_surf"),
        avg(col("`data.offset_pressure_3h_zipper`")).alias("avg_offset_pressure_3h_zipper"),
        avg(col("`data.offset_pressure_4h_zipper`")).alias("avg_offset_pressure_4h_zipper"),
        avg(col("`data.offset_pressure_5h_zipper`")).alias("avg_offset_pressure_5h_zipper"),
        avg(col("`data.extra_clean_fluid`")).alias("avg_extra_clean_fluid"),
        avg(col("`data.backside_pressure`")).alias("avg_backside_pressure")
      )
      .withColumn("fecha_central", 
                  expr("element_at(array_sort(datetime_list), greatest(1, cast(size(datetime_list)/2 as int)))"))  // Fecha central
      .withColumn("_id_central", 
                  expr("element_at(array_sort(ids), greatest(1, cast(size(ids)/2 as int)))"))   // _id central
    
    // Seleccionamos las columnas finales
    val dfFinal = dfResampled.select(
    col("stage_number"),
    col("asset_id"),
    col("fecha_central").alias("datetime"),
    col("_id_central").alias("_id"),
    col("avg_wellhead_pressure").alias("data.wellhead_pressure"),
    col("avg_slurry_flow_rate_in").alias("data.slurry_flow_rate_in"),
    col("avg_elapsed_time").alias("data.elapsed_time"),
    col("avg_clean_flow_rate_in").alias("data.clean_flow_rate_in"),
    col("avg_total_clean_volume_in").alias("data.total_clean_volume_in"),
    col("avg_total_slurry_volume_in").alias("data.total_slurry_volume_in"),
    col("avg_total_chemical_rate_in").alias("data.total_chemical_rate_in"),
    col("avg_total_friction_reducer").alias("data.total_friction_reducer"),
    col("avg_total_proppant_concentration").alias("data.total_proppant_concentration"),
    col("avg_total_proppant_mass").alias("data.total_proppant_mass"),
    col("avg_bottomhole_proppant_concentration").alias("data.bottomhole_proppant_concentration"),
    col("avg_hydrostatic_pressure").alias("data.hydrostatic_pressure"),
    col("avg_inverse_hydrostatic_pressure").alias("data.inverse_hydrostatic_pressure"),
    col("avg_enzyme_breaker").alias("data.enzyme_breaker"),
    col("avg_scale_inhibitor").alias("data.scale_inhibitor"),
    col("avg_surfactant").alias("data.surfactant"),
    col("avg_friction_reducer").alias("data.friction_reducer"),
    col("avg_friction_reducer_extra").alias("data.friction_reducer_extra"),
    col("avg_cross_linker").alias("data.cross_linker"),
    col("avg_acid").alias("data.acid"),
    col("avg_gel").alias("data.gel"),
    col("avg_ph_adjusting_agent").alias("data.ph_adjusting_agent"),
    col("avg_accelerator").alias("data.accelerator"),
    col("avg_fluid_loss").alias("data.fluid_loss"),
    col("avg_ploymer_plug").alias("data.ploymer_plug"),
    col("avg_acid_inhibitor").alias("data.acid_inhibitor"),
    col("avg_acid_retarder").alias("data.acid_retarder"),
    col("avg_emulsifier").alias("data.emulsifier"),
    col("avg_clay_stabilizer").alias("data.clay_stabilizer"),
    col("avg_non_emulsifier").alias("data.non_emulsifier"),
    col("avg_fines_suspender").alias("data.fines_suspender"),
    col("avg_anti_sludge").alias("data.anti_sludge"),
    col("avg_iron_control").alias("data.iron_control"),
    col("avg_oxygen_scavenger").alias("data.oxygen_scavenger"),
    col("avg_mutual_solvent").alias("data.mutual_solvent"),
    col("avg_corrosion_inhibitor").alias("data.corrosion_inhibitor"),
    col("avg_paraffin_control").alias("data.paraffin_control"),
    col("avg_biocide").alias("data.biocide"),
    col("avg_instant_crosslinker").alias("data.instant_crosslinker"),
    col("avg_delayed_crosslinker").alias("data.delayed_crosslinker"),
    col("avg_liquid_breaker").alias("data.liquid_breaker"),
    col("avg_powder_breaker").alias("data.powder_breaker"),
    col("avg_divertor").alias("data.divertor"),
    col("avg_powder_gel").alias("data.powder_gel"),
    col("avg_powder_friction_reducer").alias("data.powder_friction_reducer"),
    col("avg_powder_enzyme_breaker").alias("data.powder_enzyme_breaker"),
    col("avg_proppant_1_concentration").alias("data.proppant_1_concentration"),
    col("avg_proppant_2_concentration").alias("data.proppant_2_concentration"),
    col("avg_proppant_1_mass").alias("data.proppant_1_mass"),
    col("avg_delayed_crosslink").alias("data.delayed_crosslink"),
    col("avg_pumpside_pressure").alias("data.pumpside_pressure"),
    col("avg_offset_pressure_3h_surf").alias("data.offset_pressure_3h_surf"),
    col("avg_offset_pressure_4h_surf").alias("data.offset_pressure_4h_surf"),
    col("avg_offset_pressure_5h_surf").alias("data.offset_pressure_5h_surf"),
    col("avg_offset_pressure_3h_zipper").alias("data.offset_pressure_3h_zipper"),
    col("avg_offset_pressure_4h_zipper").alias("data.offset_pressure_4h_zipper"),
    col("avg_offset_pressure_5h_zipper").alias("data.offset_pressure_5h_zipper"),
    col("avg_extra_clean_fluid").alias("data.extra_clean_fluid"),
    col("avg_backside_pressure").alias("data.backside_pressure")
    )

    dfFinal
}




// COMMAND ----------

import org.apache.spark.sql.functions._
//Limpiamos api_number de assets
val dfAs: DataFrame = getDataFromSource(sourcesFinal("aa001129_usa_assets"))
val dfAs2 = dfAs.withColumn("api_number", trim(regexp_replace(col("api_number"), "\\s+", "")))
val dfCleanedAs = dfAs2.withColumn("api_number", regexp_replace(col("api_number"), "[^0-9]", ""))

//cod_api12 a cod_api10 en wellbore
val dfAria: DataFrame = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_wellbore_un"))
val dfAria2 = dfAria.withColumn("cod_api12", trim(regexp_replace(col("cod_api12"), "\\s+", "")))
val dfExtracted = dfAria2.withColumn("cod_api10", substring(col("cod_api12"), 1, 10))

//Join de api number assets - welbore
val dfMatches = dfExtracted.join(dfCleanedAs, dfExtracted("cod_api10") === dfCleanedAs("api_number"))
val dfSelected = dfMatches.select("id_vers_rel", "asset_id", "api_number", "fec_start_date_rel", "cod_uwbi", "id_vers_well", "id_vers_well_hole")


// COMMAND ----------

def filterMostRecentMatches(df1: DataFrame, df2: DataFrame): DataFrame = {
  // Crear la ventana particionada por api_number y ordenada por fec_start_date_rel en orden descendente
  val ventana = Window.partitionBy("cod_wits" ,"asset_id_wits", "stage_number").orderBy(desc("fec_start_date_rel"))
  val df2r = df2.withColumnRenamed("asset_id", "asset_id_wits").withColumnRenamed("_id", "cod_wits")

  // Hacer el join entre df1 y df2 basándose en asset_id
  val dfFiltered = df2r.join(df1, df2r("asset_id_wits") === df1("asset_id"),"left").withColumn("cod_vers_wellbore", coalesce(df1("id_vers_rel"), lit("-99"))).withColumn("cod_vers_well_string", coalesce(df1("id_vers_well"), lit("-99"))).withColumn("cod_vers_well", coalesce(df1("id_vers_well_hole"), lit("-99")))

  // Añadir la columna row_number a df1
  val dfFinal = dfFiltered.withColumn("row_num", row_number().over(ventana)).filter(col("row_num") === 1)


  dfFinal
}


// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

def CalculoPumping(dfTransformed: DataFrame): DataFrame = {
  
  // 1. Cargar el dataframe dfprediction
  val dfprediction: DataFrame = getDataFromSource(sourcesFinal("aa001129_usa_completion-predictions"))
  
  // 2. Filtrar dfprediction por asset_id en dfTransformed y también por num_stage y stage_number
  val assetIdsAndStages = dfTransformed.select("asset_id", "num_stage").distinct()
  
  // Renombrar asset_id a asset_prediction en dfprediction para evitar ambigüedad
  val dfpredictionRenamed = dfprediction.withColumnRenamed("asset_id", "asset_prediction")
  
  // Realizar el join con el dfprediction renombrado
  val dfFilteredPrediction = dfpredictionRenamed
    .join(assetIdsAndStages, 
          dfpredictionRenamed("asset_prediction") === assetIdsAndStages("asset_id") && 
          dfpredictionRenamed("stage_number") === assetIdsAndStages("num_stage"))

  // 3. Crear la nueva columna val_time_start_pump sumando los campos y convirtiendo a minutos
  val dfWithTimeStartPump = dfFilteredPrediction.withColumn(
    "val_pump_elapsed_time", 
    ($"`data.main_pumping_start_timestamp`" + $"`data.pumping_duration`") / 60
  )
  // 4. Añadir la nueva columna a dfTransformed
  val dfResult = dfTransformed.join(
    dfWithTimeStartPump.select("asset_prediction", "stage_number", "val_pump_elapsed_time"),
    dfTransformed("asset_id") === dfWithTimeStartPump("asset_prediction") && 
    dfTransformed("num_stage") === dfWithTimeStartPump("stage_number"),
    "left"
  ).drop("asset_prediction","stage_number", "asset_id") // Eliminar la columna renombrada si no es necesaria

  // Devolver el dataframe final con la nueva columna
  dfResult
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

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {   

        val dfwitsfulloInc = transformFullorInc(getDataFromSource(sourcesFinal("aa001129_usa_completion-wits")))

        val dfwitsresample = Resample(dfwitsfulloInc)
        val dfwitsFI = dfwitsresample.withColumn("__calculated_key__", col("_id"))

        val dfResult = filterMostRecentMatches(dfSelected, dfwitsFI)

        val dfTransformed = transformationdf(dfResult)

        val dfwithprediction = CalculoPumping(dfTransformed)

        val dfFinal = dfwithprediction.select(order.map(col): _*)

        df = dfFinal.withColumn("val_pump_elapsed_time", col("val_pump_elapsed_time").cast("Double"))

        df = addCodUwbiNumStage(df, "cod_uwbi", "num_stage", "cod_uwbi_num_stage")

        val (targetDf, targetDelta) = getDeltaTable(df, targetFinal)
        applyChange(targetFinal, df)
    
        optimizeDeltaFun(targetDelta, getObject(targetFinal("target_object").toString) ,auxParamsFinal("dayOfWeek").toString)

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


val result = s"""{"error": "$error"}"""

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

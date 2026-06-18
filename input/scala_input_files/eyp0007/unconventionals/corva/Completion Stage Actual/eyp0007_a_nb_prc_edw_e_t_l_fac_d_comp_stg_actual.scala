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
val order = Seq("cod_vers_wellbore", "cod_vers_well_string", "cod_vers_well", "cod_vers", "cod_uwbi","cod_uwbi_num_stage", "num_stage","id_asset","val_interval_top","val_interval_base","fec_time_start","fec_time_end","val_pump_duration","val_prefrac_static_wh_press","val_perfrac_isip",
"val_postfrac_isip","avg_treating_pressure","max_treating_pressure","val_breakdown_pressure","avg_pump_rate","max_pump_rate","val_stage_acid_volume",
"val_stage_slurry_volume","val_stage_clean_fluid","val_stage_slickwater_volume","val_stage_linear_gel_volume","val_stage_crosslnk_gel_volume",
"val_stage_100mesh_sand_volume","val_stage_40_70_sand_volume","val_stage_30_50_sand_volume","val_stage_20_40_sand_volume","val_total_proppant","val_fresh_water","val_15hcl_acid",
"val_time_between_stg","val_press_decline_btw_stg","val_rt_press_decline_btw_stg"
)

// Renombramientos para dfFinal
def transformationdf(df: DataFrame): DataFrame = {
  var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("stage_number_prediction", "num_stage")
               .withColumnRenamed("asset_id_prediction", "id_asset")
               .withColumnRenamed("data.top_perforation_stages", "val_interval_top")
               .withColumnRenamed("data.bottom_perforation_stages", "val_interval_base")
               .withColumnRenamed("data.start_time_stages", "fec_time_start")
               .withColumnRenamed("data.end_time_stages", "fec_time_end")
               .withColumnRenamed("data.pumping_duration_prediction", "val_pump_duration")
               .withColumnRenamed("data.closed_wellhead_pressure_stages", "val_prefrac_static_wh_press")
               .withColumnRenamed("wellhead_pressure_opening", "val_perfrac_isip")
               .withColumnRenamed("wellhead_pressure_isip", "val_postfrac_isip")
               .withColumnRenamed("data.average_treating_pressure_stages", "avg_treating_pressure")
               .withColumnRenamed("data.max_treating_pressure_stages", "max_treating_pressure")
               .withColumnRenamed("data.break_down_stages", "val_breakdown_pressure")
               .withColumnRenamed("data.average_pumping_rate_stages", "avg_pump_rate")
               .withColumnRenamed("data.max_pumping_rate_stages", "max_pump_rate")
               .withColumnRenamed("data.cumulative_chemicals.acid_prediction", "val_stage_acid_volume")
               .withColumnRenamed("data.total_slurry_volume_prediction", "val_stage_slurry_volume")
               .withColumnRenamed("data.total_clean_volume_prediction", "val_stage_clean_fluid")
               .withColumnRenamed("data.cumulative_chemicals.gel_prediction", "val_stage_linear_gel_volume")
               .withColumnRenamed("data.cumulative_chemicals.cross_linker_prediction", "val_stage_crosslnk_gel_volume")
               .withColumnRenamed("data.total_proppant_mass_prediction", "val_total_proppant")
               
  return dfTrn
}

def Createcodvers(df: DataFrame): DataFrame = {
  val dfConCodVers = df.withColumn(
    "cod_vers",
    concat(col("cod_vers_wellbore"), col("stage_number_prediction").cast("string"), col("asset_id_prediction").cast("string"))
  )
  dfConCodVers
}

val rowDummy = Row("-99", "-99","-99","-99", null, null, null, null, null, null, null, null, null, null,  null, null,  
null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
null, null, null) 


def addRowDummy(df: DataFrame): DataFrame = {
    
    var dfTrn: DataFrame = df

    val dfDummy = spark.createDataFrame(Seq(rowDummy).asJava, df.schema)

    dfTrn = dfTrn.union(dfDummy)

    return dfTrn

}

// COMMAND ----------

print(sourcesFinal)

// COMMAND ----------


//Limpiamos api_number de assets
val dfAs: DataFrame = getDataFromSource(sourcesFinal("`aa001129_usa_assets`"))
val dfAs2 = dfAs.withColumn("api_number", trim(regexp_replace(col("api_number"), "\\s+", "")))
val dfCleanedAs = dfAs.withColumn("api_number", regexp_replace(col("api_number"), "[^0-9]", ""))

//cod_api12 a cod_api10 en wellbore
val dfAria: DataFrame = getDataFromSource(sourcesFinal("`eyp0007_tb_dim_d_wellbore_un`"))
val dfAria2 = dfAria.withColumn("cod_api12", trim(regexp_replace(col("cod_api12"), "\\s+", "")))
val dfExtracted = dfAria.withColumn("cod_api10", substring(col("cod_api12"), 1, 10))

//Join de api number assets - welbore
val dfMatches = dfExtracted.join(dfCleanedAs, dfExtracted("cod_api10") === dfCleanedAs("api_number"))
val dfSelected = dfMatches.select("id_vers_rel", "asset_id", "api_number", "fec_start_date_rel", "cod_uwbi", "id_vers_well", "id_vers_well_hole")
val dfAssetWelbore = dfSelected.withColumnRenamed("asset_id","asset_id_wellbore")

// COMMAND ----------

//Unión de stages y predictions
val dfstage: DataFrame = getDataFromSource(sourcesFinal("`aa001129_usa_completion-data-actual-stages`"))
val dfRenamedstages = dfstage.columns.foldLeft(dfstage) { (tempDf, colName) =>
  tempDf.withColumnRenamed(colName, s"${colName}_stages")
}
val dfprediction: DataFrame = getDataFromSource(sourcesFinal("`aa001129_usa_completion-predictions`"))
val dfRenamedprediction = dfprediction.columns.foldLeft(dfprediction) { (tempDf, colName) =>
  tempDf.withColumnRenamed(colName, s"${colName}_prediction")
}
val joinedDF = dfRenamedprediction.join(
    dfRenamedstages,
    dfRenamedstages("asset_id_stages") === dfRenamedprediction("asset_id_prediction") &&
    dfRenamedstages("`data.stage_number_stages`") === dfRenamedprediction("stage_number_prediction"),
    "inner"  
)
//Conversión data.start_time_stages para solo coger el mayor o igual al fec_start_date_rel de wellbore
val dfstageprediction = joinedDF.withColumn("data.start_time_stages",to_timestamp(from_unixtime(col("`data.start_time_stages`"))))

// Crear la ventana particionada por api_number y ordenada por fec_start_date_rel en orden descendente
val windowdateasset = Window.partitionBy("asset_id_prediction", "stage_number_prediction").orderBy(desc("fec_start_date_rel"))

val dfFiltered = dfstageprediction.join(dfAssetWelbore, dfstageprediction("asset_id_prediction") === dfAssetWelbore("asset_id_wellbore"),"left")
.withColumn("cod_vers_wellbore", coalesce(dfAssetWelbore("id_vers_rel"), lit("-99"))).withColumn("cod_vers_well_string", coalesce(dfAssetWelbore("id_vers_well"), lit("-99"))).withColumn("cod_vers_well", coalesce(dfAssetWelbore("id_vers_well_hole"), lit("-99")))

// Filtrar solo la fila con row_number = 1
val dfFinal = dfFiltered.withColumn("row_num", row_number().over(windowdateasset)).filter(col("row_num") === 1)



// COMMAND ----------

def createUnionColumnpropants(dfFinal: DataFrame): DataFrame = {

    val dfproppants: DataFrame = getDataFromSource(sourcesFinal("`am_eyp0005_usa_completion-data-actual-stages-proppants`"))
    val dfPropRe = dfproppants.select(
        col("asset_id").as("asset_id_proppants"),
        col("`data.stage_number`").as("data.stage_number_proppants"),
        col("amount").as("amount_proppants"),
        col("type").as("type_proppants")
        )

    // Hacer el join
    val dfFunion = dfFinal
    .join(dfPropRe, 
            dfFinal("asset_id_prediction") === dfPropRe("asset_id_proppants") && 
            dfFinal("stage_number_prediction") === dfPropRe("`data.stage_number_proppants`"), 
            "left") // Mantener todas las filas de dfFinal

    // Rellenar las columnas según el tipo de proppant
    val resultDF = dfFunion
    .withColumn("val_stage_100mesh_sand_volume",
                when(col("type_proppants").contains("100"), col("amount_proppants")).otherwise(null))
    .withColumn("val_stage_40_70_sand_volume",
                when(col("type_proppants").contains("40/70"), col("amount_proppants")).otherwise(null))
    .withColumn("val_stage_30_50_sand_volume",
                when(col("type_proppants").contains("30/50"), col("amount_proppants")).otherwise(null))
    .withColumn("val_stage_20_40_sand_volume",
                when(col("type_proppants").contains("20/40"), col("amount_proppants")).otherwise(null))
    .groupBy(dfFinal("stage_number_prediction"), dfFinal("asset_id_prediction"))
    .agg(
        coalesce(first("val_stage_100mesh_sand_volume", ignoreNulls = true), lit(0)).alias("val_stage_100mesh_sand_volume"),
        coalesce(first("val_stage_40_70_sand_volume", ignoreNulls = true), lit(0)).alias("val_stage_40_70_sand_volume"),
        coalesce(first("val_stage_30_50_sand_volume", ignoreNulls = true), lit(0)).alias("val_stage_30_50_sand_volume"),
        coalesce(first("val_stage_20_40_sand_volume", ignoreNulls = true), lit(0)).alias("val_stage_20_40_sand_volume")
    )

    val renamedDF = resultDF.withColumnRenamed("stage_number_prediction", "stage_number_proppants").withColumnRenamed("asset_id_prediction", "asset_id_proppants")
    val dfunionfinal = dfFinal
    .join(renamedDF, 
            dfFinal("asset_id_prediction") === renamedDF("asset_id_proppants") && 
            dfFinal("stage_number_prediction") === renamedDF("stage_number_proppants"), 
            "left")

    dfunionfinal
    }

// COMMAND ----------

def createUnionColumnfluids(dfFinal: DataFrame): DataFrame = {

    val dffluids: DataFrame = getDataFromSource(sourcesFinal("`am_eyp0005_usa_completion-data-actual-stages-fluids`"))
    val dffluiRe = dffluids.select(
        col("asset_id").as("asset_id_fluids"),
        col("`data.stage_number`").as("data.stage_number_fluids"),
        col("amount").as("amount_fluids"),
        col("type").as("type_fluids")
        )
    //Insertar filas nuevas val_stage_slickwater_volume y 
    // Hacer el join
    val dfFunion = dfFinal
    .join(dffluiRe, 
            dfFinal("asset_id_prediction") === dffluiRe("asset_id_fluids") && 
            dfFinal("stage_number_prediction") === dffluiRe("`data.stage_number_fluids`"), 
            "left") // Mantener todas las filas de dfFinal

    // Rellenar las columnas según el tipo de proppant
    val resultDF = dfFunion
    .withColumn("val_stage_slickwater_volume",
                when(col("type_fluids").contains("Slickwater"), col("amount_fluids")).otherwise(null))
    .withColumn("val_fresh_water",
                when(col("type_fluids").contains("Fresh Water"), col("amount_fluids")).otherwise(null))
    .withColumn("val_flowback_water",
                when(col("type_fluids").contains("Flowback Water"), col("amount_fluids")).otherwise(null))
    .withColumn("val_15hcl_acid",
                when(col("type_fluids").contains("15% HCL Acid"), col("amount_fluids")).otherwise(null))
    .groupBy(dfFinal("stage_number_prediction"), dfFinal("asset_id_prediction"))
    .agg(
        coalesce(first("val_stage_slickwater_volume", ignoreNulls = true), lit(0)).alias("val_stage_slickwater_volume"),
        coalesce(first("val_fresh_water", ignoreNulls = true), lit(0)).alias("val_fresh_water"),
        coalesce(first("val_flowback_water", ignoreNulls = true), lit(0)).alias("val_flowback_water"),
        coalesce(first("val_15hcl_acid", ignoreNulls = true), lit(0)).alias("val_15hcl_acid")
    )

    val renamedDF = resultDF.withColumnRenamed("stage_number_prediction", "stage_number_fluids").withColumnRenamed("asset_id_prediction", "asset_id_fluids")
    val dfunionfinal = dfFinal
    .join(renamedDF, 
            dfFinal("asset_id_prediction") === renamedDF("asset_id_fluids") && 
            dfFinal("stage_number_prediction") === renamedDF("stage_number_fluids"), 
            "left")
    dfunionfinal
    }

// COMMAND ----------

def addTimeBetweenStages(df: DataFrame): DataFrame = {
  // Definir la ventana para cada asset_id_prediction y ordenar por stage_number_prediction
  val windowSpec = Window.partitionBy("asset_id_prediction").orderBy("stage_number_prediction")
  val dfReverted = df.withColumn("data.start_time_stages", unix_timestamp(col("`data.start_time_stages`")))
  
  // Calcular el tiempo entre etapas y crear la nueva columna
  val dfWithTimeBetweenStg = dfReverted
    .withColumn("prev_fec_time_end", lag("`data.end_time_stages`", 1).over(windowSpec))
    .withColumn("val_time_between_stg", col("`data.start_time_stages`") - col("prev_fec_time_end"))
  dfWithTimeBetweenStg

  
}

// COMMAND ----------

def addPressureDeclineBetweenStage(df: DataFrame): DataFrame = {

    val dfCombined = df.withColumn("wellhead_pressure_isip", regexp_extract(col("`data.isip_prediction`"), """wellhead_pressure': ([\d.]+)""", 1)).withColumn("wellhead_pressure_opening", regexp_extract(col("`data.opening_wellhead_pressure_prediction`"), """wellhead_pressure': ([\d.]+)""", 1))

    // Crear una ventana para el cálculo del siguiente stage_number
    val windowSpec = Window.partitionBy("asset_id_prediction").orderBy("stage_number_prediction")

    // Obtener el wellhead_pressure del siguiente stage_number
    val dfWithNextStagePressure = dfCombined.withColumn(
    "wellhead_pressure_next_stage",
    lead("wellhead_pressure_opening", 1).over(windowSpec)
    )

    // Calcular la diferencia y añadirla como una nueva columna
    val dfFina: DataFrame = dfWithNextStagePressure.withColumn(
    "val_press_decline_btw_stg",
    when(col("wellhead_pressure_next_stage").isNull, col("wellhead_pressure_isip"))
        .otherwise(col("wellhead_pressure_isip") - col("wellhead_pressure_next_stage"))
    )

    // Calcular la nueva columna val_rt_press_decline_btw_stg
    val dfFinalResults = dfFina.withColumn(
    "val_rt_press_decline_btw_stg",
    when(col("val_time_between_stg").isNull || col("val_time_between_stg") === 0, null) // Manejar cero y nulos
        .otherwise(col("val_press_decline_btw_stg") / col("val_time_between_stg"))
    )

    dfFinalResults
}

// COMMAND ----------

def convertDataTypes(df: DataFrame): DataFrame = {
  df.withColumn("fec_time_start", to_timestamp(col("fec_time_start").cast("long"))) // Convertir long a timestamp
    .withColumn("fec_time_end", to_timestamp(col("fec_time_end").cast("integer"))) // Convertir integer a timestamp
    .withColumn("val_time_between_stg", col("val_time_between_stg").cast("Double")) // Convertir long a Double
    .withColumn("val_press_decline_btw_stg", col("val_press_decline_btw_stg").cast("Double")) // Convertir double a Double
    .withColumn("val_rt_press_decline_btw_stg", col("val_rt_press_decline_btw_stg").cast("Double")) // Convertir long a Double
    .withColumn("val_postfrac_isip", col("val_postfrac_isip").cast("Double")) // Convertir long a Double
    .withColumn("val_perfrac_isip", col("val_perfrac_isip").cast("Double")) // Convertir long a Double
    .withColumn("val_pump_duration", col("val_pump_duration").cast("Double"))

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
        val dfUnionWithproppants = createUnionColumnpropants(dfFinal)

        val dfUnionwithfluidsandproppants = createUnionColumnfluids(dfUnionWithproppants)
println("a")
        val dfwithcalculo = addTimeBetweenStages(dfUnionwithfluidsandproppants)
println("a")
        val dfpressure = addPressureDeclineBetweenStage(dfwithcalculo)
println("a")
        val dfWithCodvers = Createcodvers(dfpressure)
println("a")
        val dfRename = transformationdf(dfWithCodvers)
println("a")
        val dataframeFinal = convertDataTypes(dfRename)
println("a")
        val dataframesemiFinal = addCodUwbiNumStage(dataframeFinal, "cod_uwbi", "num_stage", "cod_uwbi_num_stage")
println("a")
        val dfdf = dataframesemiFinal.select(order.map(col): _*)
        display(dfdf)
        df = addRowDummy(dfdf)

        val count= df.count()
        println(count)
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

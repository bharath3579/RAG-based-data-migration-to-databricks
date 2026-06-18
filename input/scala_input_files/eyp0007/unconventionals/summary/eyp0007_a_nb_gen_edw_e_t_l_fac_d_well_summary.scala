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
// MAGIC # Función de production Summary

// COMMAND ----------


import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.types._

def generateProdSummay (df: DataFrame): DataFrame = {


// Paso 1: Agregar los valores duplicados por id_well y fec_production_day
val aggregatedDF = df.groupBy("id_well", "fec_production_day")
  .agg(
    sum("ind_alloc_cond_vol_stb").as("ind_alloc_cond_vol_stb"),
    sum("ind_alloc_gas_vol_mscf").as("ind_alloc_gas_vol_mscf"),
    sum("ind_alloc_oil_vol_stb").as("ind_alloc_oil_vol_stb"),
    sum("ind_alloc_water_vol_bbl").as("ind_alloc_water_vol_bbl")
  ).persist()  // Persistimos este DataFrame porque lo reutilizaremos

// Paso 2: Cálculo de la primera fecha de producción para cada id_well
val windowSpecFirstProduction = Window.partitionBy("id_well").orderBy("fec_production_day")

val dfWithFirstProductionDay = aggregatedDF.withColumn("first_production_day", first("fec_production_day").over(windowSpecFirstProduction))
                                           .withColumn("last_production_day", last("fec_production_day").over(windowSpecFirstProduction))   // Última fecha de producción
                                           .withColumn("num_days_producing", datediff(col("last_production_day"), col("first_production_day")) + 1)  // Días de producción (agregamos +1 para incluir el primer día)
                                           .persist()

// Paso 3: Cálculo del valor máximo y la fecha donde ocurre utilizando row_number()
val windowSpecMax = Window.partitionBy("id_well").orderBy(col("ind_alloc_gas_vol_mscf").desc_nulls_last)

val dfWithMaxValues = dfWithFirstProductionDay
  .withColumn("row_number", row_number().over(windowSpecMax)) // Calculamos el número de fila del valor máximo
  .withColumn("max_gas_vol", max("ind_alloc_gas_vol_mscf").over(Window.partitionBy("id_well"))) // Valor máximo de gas
  .withColumn("max_gas_vol_day", when(col("max_gas_vol").isNotNull && col("row_number") === 1, col("fec_production_day"))) // Capturamos la fecha cuando el valor es máximo
  .withColumn("max_gas_vol_day", first("max_gas_vol_day", ignoreNulls=true).over(Window.partitionBy("id_well"))) // Propagamos la fecha no nula del máximo de gas
  .drop("row_number") // Eliminamos la columna auxiliar
  .persist()

// Repetimos el proceso para condensado, petróleo y agua
val windowSpecCond = Window.partitionBy("id_well").orderBy(col("ind_alloc_cond_vol_stb").desc_nulls_last)
val windowSpecOil = Window.partitionBy("id_well").orderBy(col("ind_alloc_oil_vol_stb").desc_nulls_last)
val windowSpecWater = Window.partitionBy("id_well").orderBy(col("ind_alloc_water_vol_bbl").desc_nulls_last)

val dfWithAllMaxValues = dfWithMaxValues
  .withColumn("row_number_cond", row_number().over(windowSpecCond))
  .withColumn("max_cond_vol", max("ind_alloc_cond_vol_stb").over(Window.partitionBy("id_well")))
  .withColumn("max_cond_vol_day", when(col("max_cond_vol").isNotNull && col("row_number_cond") === 1, col("fec_production_day")))
  .withColumn("max_cond_vol_day", first("max_cond_vol_day", ignoreNulls=true).over(Window.partitionBy("id_well")))
  .drop("row_number_cond")
  .withColumn("row_number_oil", row_number().over(windowSpecOil))
  .withColumn("max_oil_vol", max("ind_alloc_oil_vol_stb").over(Window.partitionBy("id_well")))
  .withColumn("max_oil_vol_day", when(col("max_oil_vol").isNotNull && col("row_number_oil") === 1, col("fec_production_day")))
  .withColumn("max_oil_vol_day", first("max_oil_vol_day", ignoreNulls=true).over(Window.partitionBy("id_well")))
  .drop("row_number_oil")
  .withColumn("row_number_water", row_number().over(windowSpecWater))
  .withColumn("max_water_vol", max("ind_alloc_water_vol_bbl").over(Window.partitionBy("id_well")))
  .withColumn("max_water_vol_day", when(col("max_water_vol").isNotNull && col("row_number_water") === 1, col("fec_production_day")))
  .withColumn("max_water_vol_day", first("max_water_vol_day", ignoreNulls=true).over(Window.partitionBy("id_well")))
  .drop("row_number_water")


// Paso 4: Cálculo del ratio promedio de condensado/gas en los primeros 15 días de producción
val first15DaysWindow = Window.partitionBy("id_well").orderBy("fec_production_day").rowsBetween(Window.unboundedPreceding, 14)

val avgCondGasRatioDF = dfWithAllMaxValues
  .withColumn("cond_gas_ratio", 
    when(col("ind_alloc_gas_vol_mscf") =!= 0 && col("ind_alloc_gas_vol_mscf").isNotNull, 
         col("ind_alloc_cond_vol_stb") / col("ind_alloc_gas_vol_mscf"))
    .otherwise(lit(null)))
  .withColumn("avg_cond_gas_ratio_15days", avg("cond_gas_ratio").over(first15DaysWindow))

// Paso 5: Cálculo de los acumulados totales y mensuales

// Ventana para acumulados
val monthlyWindow = Window.partitionBy("id_well").orderBy("fec_production_day").rangeBetween(Window.unboundedPreceding, Window.currentRow)

// Acumulado total hasta la fecha actual para los cuatro atributos
val accumTotal = avgCondGasRatioDF
  .withColumn("cumulative_cond_vol_total", sum("ind_alloc_cond_vol_stb").over(monthlyWindow))
  .withColumn("cumulative_gas_vol_total", sum("ind_alloc_gas_vol_mscf").over(monthlyWindow))
  .withColumn("cumulative_oil_vol_total", sum("ind_alloc_oil_vol_stb").over(monthlyWindow))
  .withColumn("cumulative_water_vol_total", sum("ind_alloc_water_vol_bbl").over(monthlyWindow))

// Derivar acumulados mensuales (1, 3, 6, 12, 18, y 24 meses) a partir del acumulado total
val accumFiltered = accumTotal
  .withColumn("cumulative_cond_vol_1_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 30, col("cumulative_cond_vol_total")))
  .withColumn("cumulative_cond_vol_3_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 90, col("cumulative_cond_vol_total")))
  .withColumn("cumulative_cond_vol_6_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 180, col("cumulative_cond_vol_total")))
  .withColumn("cumulative_cond_vol_12_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 365, col("cumulative_cond_vol_total")))
  .withColumn("cumulative_cond_vol_18_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 547, col("cumulative_cond_vol_total")))
  .withColumn("cumulative_cond_vol_24_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 730, col("cumulative_cond_vol_total")))
  .withColumn("cumulative_gas_vol_1_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 30, col("cumulative_gas_vol_total")))
  .withColumn("cumulative_gas_vol_3_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 90, col("cumulative_gas_vol_total")))
  .withColumn("cumulative_gas_vol_6_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 180, col("cumulative_gas_vol_total")))
  .withColumn("cumulative_gas_vol_12_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 365, col("cumulative_gas_vol_total")))
  .withColumn("cumulative_gas_vol_18_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 547, col("cumulative_gas_vol_total")))
  .withColumn("cumulative_gas_vol_24_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 730, col("cumulative_gas_vol_total")))
  .withColumn("cumulative_oil_vol_1_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 30, col("cumulative_oil_vol_total")))
  .withColumn("cumulative_oil_vol_3_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 90, col("cumulative_oil_vol_total")))
  .withColumn("cumulative_oil_vol_6_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 180, col("cumulative_oil_vol_total")))
  .withColumn("cumulative_oil_vol_12_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 365, col("cumulative_oil_vol_total")))
  .withColumn("cumulative_oil_vol_18_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 547, col("cumulative_oil_vol_total")))
  .withColumn("cumulative_oil_vol_24_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 730, col("cumulative_oil_vol_total")))
  .withColumn("cumulative_water_vol_1_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 30, col("cumulative_water_vol_total")))
  .withColumn("cumulative_water_vol_3_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 90, col("cumulative_water_vol_total")))
  .withColumn("cumulative_water_vol_6_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 180, col("cumulative_water_vol_total")))
  .withColumn("cumulative_water_vol_12_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 365, col("cumulative_water_vol_total")))
  .withColumn("cumulative_water_vol_18_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 547, col("cumulative_water_vol_total")))
  .withColumn("cumulative_water_vol_24_month", when(datediff(col("fec_production_day"), col("first_production_day")) <= 730, col("cumulative_water_vol_total")))

// Paso 6: Agrupación Final para Consolidar un Solo Registro por id_well

val finalDF = accumFiltered
  .groupBy("id_well")
  .agg(
    first("first_production_day").as("fec_first_prod_day"),
    max("num_days_producing").as("num_days_producing"),
    first("max_cond_vol").as("val_max_cond_vol_stb"),
    first("max_cond_vol_day").as("fec_max_cond_vol"),
    first("max_gas_vol_day").as("fec_max_gas_vol"),
    first("max_gas_vol").as("val_max_gas_vol_mscf"),
    first("max_oil_vol_day").as("fec_max_oil_vol"),
    first("max_oil_vol").as("val_max_oil_vol_stb"),
    first("max_water_vol_day").as("fec_max_water_vol"),
    first("max_water_vol").as("val_max_water_vol_bbl"),
    max("avg_cond_gas_ratio_15days").as("avg_cond_gas_rt_15days"),
    max("cumulative_cond_vol_1_month").as("val_acum_cond_vol_1_m_stb"),
    max("cumulative_cond_vol_3_month").as("val_acum_cond_vol_3_m_stb"),
    max("cumulative_cond_vol_6_month").as("val_acum_cond_vol_6_m_stb"),
    max("cumulative_cond_vol_12_month").as("val_acum_cond_vol_12_m_stb"),
    max("cumulative_cond_vol_18_month").as("val_acum_cond_vol_18_m_stb"),
    max("cumulative_cond_vol_24_month").as("val_acum_cond_vol_24_m_stb"),
    max("cumulative_cond_vol_total").as("val_acum_cond_vol_tot_stb"),
    max("cumulative_gas_vol_1_month").as("val_acum_gas_vol_1_m_mscf"),
    max("cumulative_gas_vol_3_month").as("val_acum_gas_vol_3_m_mscf"),
    max("cumulative_gas_vol_6_month").as("val_acum_gas_vol_6_m_mscf"),
    max("cumulative_gas_vol_12_month").as("val_acum_gas_vol_12_m_mscf"),
    max("cumulative_gas_vol_18_month").as("val_acum_gas_vol_18_m_mscf"),
    max("cumulative_gas_vol_24_month").as("val_acum_gas_vol_24_m_mscf"),
    max("cumulative_gas_vol_total").as("val_acum_gas_vol_tot_mscf"),
    max("cumulative_oil_vol_1_month").as("val_acum_oil_vol_1_m_stb"),
    max("cumulative_oil_vol_3_month").as("val_acum_oil_vol_3_m_stb"),
    max("cumulative_oil_vol_6_month").as("val_acum_oil_vol_6_m_stb"),
    max("cumulative_oil_vol_12_month").as("val_acum_oil_vol_12_m_stb"),
    max("cumulative_oil_vol_18_month").as("val_acum_oil_vol_18_m_stb"),
    max("cumulative_oil_vol_24_month").as("val_acum_oil_vol_24_m_stb"),
    max("cumulative_oil_vol_total").as("val_acum_oil_vol_tot_tsb"),
    max("cumulative_water_vol_1_month").as("val_acum_water_vol_1_m_bbl"),
    max("cumulative_water_vol_3_month").as("val_acum_water_vol_3_m_bbl"),
    max("cumulative_water_vol_6_month").as("val_acum_water_vol_6_m_bbl"),
    max("cumulative_water_vol_12_month").as("val_acum_water_vol_12_m_bbl"),
    max("cumulative_water_vol_18_month").as("val_acum_water_vol_18_m_bbl"),
    max("cumulative_water_vol_24_month").as("val_acum_water_vol_24_m_bbl"),
    max("cumulative_water_vol_total").as("val_acum_water_vol_tot_bbl")
  )

// Paso 7: Reparticionamiento final para mejorar la distribución del DataFrame final
val finalDFRepartitioned = finalDF.repartition(16, col("id_well"))

return finalDFRepartitioned

}


// COMMAND ----------

// MAGIC %md
// MAGIC # Función de cálculo de métricas de stage

// COMMAND ----------

def calculateMetrics(df: DataFrame): DataFrame = {
  import org.apache.spark.sql.functions._

  // Agrupamos por cod_uwbi
  val groupedDf = df.groupBy("cod_uwbi")

  // Cálculo del número de stages (etapas)
  val numStages = count("num_stage").as("num_stages")

  // Cálculo de la longitud total de los stages, ignorando solo los nulos
	val totalCompletedLength = sum(coalesce(col("val_completed_length"), lit(0))).as("val_completed_length")

  // Cálculo del Sand Loading (Carga de arena)
  val totalProppant = sum("val_total_proppant").as("val_total_stage_proppant")

  // Cálculo del total de la longitud de intervalos
  val totalIntervalLength = sum(col("val_interval_base") - col("val_interval_top"))
    .as("total_interval_length")

  // Cálculo del Fluid Loading (Carga de fluidos)
  val fluidLoading = sum("val_stage_slurry_volume").as("val_total_fluid_loading")

  // Aplicamos las agregaciones
  val resultDf = groupedDf.agg(
    numStages,
    totalProppant,
    totalIntervalLength,
    fluidLoading,
    totalCompletedLength
  )

  // Recalculamos val_stage_length como: val_gross_lateral_length / num_stages
  val finalDf = resultDf
    .withColumn(
      "avg_length_stages",
      when(col("num_stages") > 0, col("total_interval_length") / col("num_stages"))
        .otherwise(lit(0))
    )

  finalDf
}


// COMMAND ----------

// MAGIC %md
// MAGIC # Funciones adicionales

// COMMAND ----------

val orderFields: Seq[String] = Seq("cod_vers_wellbore", "des_well_hole", "cod_uwi", "cod_uwbi","cod_api10", "cod_api12","des_county", "des_installation_name", "fec_spud_date",
                                    "fec_completion_date","num_stages", "val_completed_length", "val_stage_length", "avg_length_stages", "val_sand_loading", "val_fluid_loading",
                                    "fec_first_prod_day", "num_days_producing","fec_max_cond_vol" ,"val_max_cond_vol_stb","fec_max_gas_vol", "val_max_gas_vol_mscf",
                                    "fec_max_oil_vol", "val_max_oil_vol_stb", "fec_max_water_vol", "val_max_water_vol_bbl", "avg_cond_gas_rt_15days", "val_acum_cond_vol_1_m_stb",
                                    "val_acum_cond_vol_3_m_stb", "val_acum_cond_vol_6_m_stb", "val_acum_cond_vol_12_m_stb", "val_acum_cond_vol_18_m_stb", "val_acum_cond_vol_24_m_stb",
                                    "val_acum_cond_vol_tot_stb", "val_acum_gas_vol_1_m_mscf", "val_acum_gas_vol_3_m_mscf", "val_acum_gas_vol_6_m_mscf", "val_acum_gas_vol_12_m_mscf",
                                    "val_acum_gas_vol_18_m_mscf", "val_acum_gas_vol_24_m_mscf", "val_acum_gas_vol_tot_mscf", "val_acum_oil_vol_1_m_stb", "val_acum_oil_vol_3_m_stb",
                                    "val_acum_oil_vol_6_m_stb", "val_acum_oil_vol_12_m_stb", "val_acum_oil_vol_18_m_stb", "val_acum_oil_vol_24_m_stb", "val_acum_oil_vol_tot_tsb",
                                    "val_acum_water_vol_1_m_bbl", "val_acum_water_vol_3_m_bbl", "val_acum_water_vol_6_m_bbl", "val_acum_water_vol_12_m_bbl", "val_acum_water_vol_18_m_bbl",
                                    "val_acum_water_vol_24_m_bbl", "val_acum_water_vol_tot_bbl", "val_acum_cond_effective_vol_1_m_stb", "val_acum_cond_effective_vol_3_m_stb", "val_acum_cond_effective_vol_6_m_stb", 
                                    "val_acum_cond_effective_vol_12_m_stb", "val_acum_cond_effective_vol_18_m_stb", "val_acum_cond_effective_vol_24_m_stb", "val_acum_cond_effective_vol_tot_stb", 
                                    "val_acum_gas_effective_vol_1_m_mscf", "val_acum_gas_effective_vol_3_m_mscf","val_acum_gas_effective_vol_6_m_mscf", "val_acum_gas_effective_vol_12_m_mscf", 
                                    "val_acum_gas_effective_vol_18_m_mscf", "val_acum_gas_effective_vol_24_m_mscf", "val_acum_gas_effective_vol_tot_mscf", "val_acum_oil_effective_vol_1_m_stb", "val_acum_oil_effective_vol_3_m_stb",
                                    "val_acum_oil_effective_vol_6_m_stb", "val_acum_oil_effective_vol_12_m_stb", "val_acum_oil_effective_vol_18_m_stb", "val_acum_oil_effective_vol_24_m_stb", "val_acum_oil_effective_vol_tot_stb",
                                    "val_acum_water_effective_vol_1_m_bbl", "val_acum_water_effective_vol_3_m_bbl", "val_acum_water_effective_vol_6_m_bbl", "val_acum_water_effective_vol_12_m_bbl", "val_acum_water_effective_vol_18_m_bbl", 
                                    "val_acum_water_effective_vol_24_m_bbl", "val_acum_water_effective_vol_tot_bbl", "val_total_stage_proppant", "val_total_fluid_loading", "val_gross_lateral_length")


def transformWell (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.orderBy(desc("fec_start_date")).dropDuplicates("cod_uwi")
                 .withColumnRenamed("id_vers", "id_vers_well_hole")

    dfTrn = dfTrn.select("id_vers_well_hole","des_well_name", "cod_uwi", "cod_api10", "des_county", "des_installation_name", "fec_spud_date")
                  .withColumnRenamed("des_well_name", "des_well_hole")

    return dfTrn
}

def transformWellbore (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.orderBy(desc("fec_start_date_rel")).dropDuplicates("cod_uwbi").withColumnRenamed("id_vers_rel", "cod_vers_wellbore").withColumnRenamed("fec_completed_date", "fec_completion_date")

    dfTrn = dfTrn.select("cod_vers_wellbore","id_well", "cod_api12", "cod_uwbi", "id_vers_well_hole", "fec_completion_date")

    return dfTrn
}

def join (dfL: DataFrame, dfR: DataFrame, keyCol: String, joinType: String): DataFrame = {

    var dfTrn: DataFrame = null

    dfTrn = dfL.join(dfR, Seq(keyCol), joinType)

    return dfTrn
}

def joinProdWellbore (dfL: DataFrame, dfR: DataFrame): DataFrame = {

   var dfTrn: DataFrame = null

   val dfWellbore: DataFrame = dfR.orderBy(desc("cod_uwbi")).dropDuplicates("id_well")

   dfTrn = dfL.join(dfWellbore, Seq("id_well"), "inner").drop("id_well")
                                                        .withColumn("id_vers_well_hole", coalesce(col("id_vers_well_hole"), lit("-99")))
                                                        .withColumn("cod_vers_wellbore", coalesce(col("cod_vers_wellbore"), lit("-99")))
   
   return dfTrn
}

def joinProdAndComp (dfL: DataFrame, dfR: DataFrame): DataFrame = {

  var dfLeft: DataFrame = null

  var dfLeftAnti: DataFrame = null

  var dfResult: DataFrame = null


  val dfLeft1=dfL.withColumnRenamed("fec_completion_date", "fec_completion_date_1")
  val dfLeftAnti1=dfR.withColumnRenamed("fec_completion_date", "fec_completion_date_2")

  dfLeft = dfLeft1.join(dfLeftAnti1, Seq("cod_vers_wellbore","cod_api12", "cod_uwbi", "id_vers_well_hole"), "left")

  dfLeftAnti = dfLeftAnti1.join(dfLeft1, Seq("cod_vers_wellbore","cod_api12", "cod_uwbi", "id_vers_well_hole"), "left_anti")

  dfResult = dfLeft.unionByName(dfLeftAnti, allowMissingColumns = true)
  dfResult = dfResult.withColumnRenamed("fec_completion_date_1", "fec_completion_date")
  
  return dfResult


}


// COMMAND ----------

def addEffectiveAccumulatedColumnsCond(df: DataFrame, dfEffect: DataFrame): DataFrame = {
  // Nombre de las columnas relevantes
  val codVersColumn = "cod_vers_wellbore"
  val indEffectiveColumn = "val_ind_effective_cond_vol_stb"
  val numDaysColumn = "num_days"
  
  // Definimos los periodos en días y los nombres de las nuevas columnas
  val periods = Seq(
    (30, "val_acum_cond_effective_vol_1_m_stb"),
    (90, "val_acum_cond_effective_vol_3_m_stb"),
    (180, "val_acum_cond_effective_vol_6_m_stb"),
    (365, "val_acum_cond_effective_vol_12_m_stb"),
    (545, "val_acum_cond_effective_vol_18_m_stb"),
    (730, "val_acum_cond_effective_vol_24_m_stb")
  )

  // Inicializamos el DataFrame de resultados
  var result = df

  // Para cada periodo, filtramos y calculamos el acumulado
  for ((days, columnName) <- periods) {
    val dfEffectFiltered = dfEffect.filter(col(numDaysColumn) <= days)
    val dfEffectAggregated = dfEffectFiltered
      .groupBy(codVersColumn)
      .agg(sum(col(indEffectiveColumn)).alias(columnName))

    // Unimos el DataFrame original con el acumulado actual
    result = result.join(dfEffectAggregated, Seq(codVersColumn), "left")
  }

  // Calculamos el acumulado total (sin límite de días)
  val totalColumn = "val_acum_cond_effective_vol_tot_stb"
  val dfEffectTotal = dfEffect
    .groupBy(codVersColumn)
    .agg(sum(col(indEffectiveColumn)).alias(totalColumn))

  // Unimos el acumulado total al resultado final
  result = result.join(dfEffectTotal, Seq(codVersColumn), "left")

  result
}

def addEffectiveAccumulatedColumnsGas(df: DataFrame, dfEffect: DataFrame): DataFrame = {
  // Nombre de la columna identificadora
  val codVersColumn = "cod_vers_wellbore"
  
  // Columna de volumen efectivo de gas
  val indEffectiveColumn = "val_ind_effective_gas_vol_mscf"
  
  // Columna de número de días
  val numDaysColumn = "num_days"

  // Definimos los periodos en días y los nombres de las nuevas columnas para gas
  val periods = Seq(
    (30,  "val_acum_gas_effective_vol_1_m_mscf"),
    (90,  "val_acum_gas_effective_vol_3_m_mscf"),
    (180, "val_acum_gas_effective_vol_6_m_mscf"),
    (365, "val_acum_gas_effective_vol_12_m_mscf"),
    (545, "val_acum_gas_effective_vol_18_m_mscf"),
    (730, "val_acum_gas_effective_vol_24_m_mscf")
  )

  // Inicializamos el DataFrame de resultados con el DataFrame base
  var result = df

  // Para cada periodo, filtramos y calculamos el acumulado
  for ((days, columnName) <- periods) {
    val dfEffectFiltered = dfEffect.filter(col(numDaysColumn) <= days)
    val dfEffectAggregated = dfEffectFiltered
      .groupBy(codVersColumn)
      .agg(sum(col(indEffectiveColumn)).alias(columnName))

    // Unimos el DataFrame original con el acumulado para este periodo
    result = result.join(dfEffectAggregated, Seq(codVersColumn), "left")
  }

  // Calculamos el acumulado total (sin límite de días)
  val totalColumn = "val_acum_gas_effective_vol_tot_mscf"
  val dfEffectTotal = dfEffect
    .groupBy(codVersColumn)
    .agg(sum(col(indEffectiveColumn)).alias(totalColumn))

  // Unimos el acumulado total al resultado final
  result = result.join(dfEffectTotal, Seq(codVersColumn), "left")

  // Devolvemos el DataFrame resultante con todas las columnas de acumulados
  result
}

def addEffectiveAccumulatedColumnsOil(df: DataFrame, dfEffect: DataFrame): DataFrame = {
  // Nombre de la columna identificadora
  val codVersColumn = "cod_vers_wellbore"
  
  // Columna de volumen efectivo de oil
  val indEffectiveColumn = "val_ind_effective_oil_vol_stb"
  
  // Columna de número de días
  val numDaysColumn = "num_days"

  // Definimos los periodos en días y los nombres de las nuevas columnas para oil
  val periods = Seq(
    (30,  "val_acum_oil_effective_vol_1_m_stb"),
    (90,  "val_acum_oil_effective_vol_3_m_stb"),
    (180, "val_acum_oil_effective_vol_6_m_stb"),
    (365, "val_acum_oil_effective_vol_12_m_stb"),
    (545, "val_acum_oil_effective_vol_18_m_stb"),
    (730, "val_acum_oil_effective_vol_24_m_stb")
  )

  // Inicializamos el DataFrame de resultados con el DataFrame base
  var result = df

  // Para cada periodo, filtramos y calculamos el acumulado
  for ((days, columnName) <- periods) {
    val dfEffectFiltered = dfEffect.filter(col(numDaysColumn) <= days)
    val dfEffectAggregated = dfEffectFiltered
      .groupBy(codVersColumn)
      .agg(sum(col(indEffectiveColumn)).alias(columnName))

    // Unimos el DataFrame original con el acumulado para este periodo
    result = result.join(dfEffectAggregated, Seq(codVersColumn), "left")
  }

  // Calculamos el acumulado total (sin límite de días)
  val totalColumn = "val_acum_oil_effective_vol_tot_stb"
  val dfEffectTotal = dfEffect
    .groupBy(codVersColumn)
    .agg(sum(col(indEffectiveColumn)).alias(totalColumn))

  // Unimos el acumulado total al resultado final
  result = result.join(dfEffectTotal, Seq(codVersColumn), "left")

  // Devolvemos el DataFrame resultante con todas las columnas de acumulados
  result
}

def addEffectiveAccumulatedColumnsWater(df: DataFrame, dfEffect: DataFrame): DataFrame = {
  // Nombre de la columna identificadora
  val codVersColumn = "cod_vers_wellbore"

  // Columna de volumen efectivo de water
  val indEffectiveColumn = "val_ind_effective_water_vol_bbl"

  // Columna de número de días
  val numDaysColumn = "num_days"

  // Definimos los periodos en días y los nombres de las nuevas columnas para water
  val periods = Seq(
    (30,  "val_acum_water_effective_vol_1_m_bbl"),
    (90,  "val_acum_water_effective_vol_3_m_bbl"),
    (180, "val_acum_water_effective_vol_6_m_bbl"),
    (365, "val_acum_water_effective_vol_12_m_bbl"),
    (545, "val_acum_water_effective_vol_18_m_bbl"),
    (730, "val_acum_water_effective_vol_24_m_bbl")
  )

  // Inicializamos el DataFrame de resultados con el DataFrame base
  var result = df

  // Para cada periodo, filtramos y calculamos el acumulado
  for ((days, columnName) <- periods) {
    val dfEffectFiltered = dfEffect.filter(col(numDaysColumn) <= days)
    val dfEffectAggregated = dfEffectFiltered
      .groupBy(codVersColumn)
      .agg(sum(col(indEffectiveColumn)).alias(columnName))

    // Unimos el DataFrame original con el acumulado para este periodo
    result = result.join(dfEffectAggregated, Seq(codVersColumn), "left")
  }

  // Calculamos el acumulado total (sin límite de días)
  val totalColumn = "val_acum_water_effective_vol_tot_bbl"
  val dfEffectTotal = dfEffect
    .groupBy(codVersColumn)
    .agg(sum(col(indEffectiveColumn)).alias(totalColumn))

  // Unimos el acumulado total al resultado final
  result = result.join(dfEffectTotal, Seq(codVersColumn), "left")

  // Devolvemos el DataFrame resultante con todas las columnas de acumulados
  result
}

// COMMAND ----------


def transformdfperf(df: DataFrame): DataFrame = {
    val dfperf = df.groupBy("cod_uwbi")
      .agg(
        min("val_perf_top").alias("min_val_perf_top"),
        max("val_perf_base").alias("max_val_perf_base")
      )
      .withColumn("val_gross_lateral_length", col("max_val_perf_base") - col("min_val_perf_top"))

    dfperf
}


def Joinperfwellb(df1: DataFrame, df2: DataFrame): DataFrame = {

    val df_join = df1.join(df2, Seq("cod_uwbi"), "left")
    
    df_join.withColumn("val_stage_length",
        when(col("num_stages") > 0, col("val_gross_lateral_length") / col("num_stages"))
          .otherwise(lit(0)))
    }

// COMMAND ----------

def brounddf(df: DataFrame): DataFrame = {

    df.withColumn("val_gross_lateral_length", bround(col("val_gross_lateral_length"), 1))
    .withColumn("val_stage_length", bround(col("val_stage_length"), 1))

}

// COMMAND ----------

def calculateLoadings(df: DataFrame): DataFrame = {
  df.withColumn(
      "val_sand_loading",
      when(col("val_gross_lateral_length") > 0,
           col("val_total_stage_proppant") / col("val_gross_lateral_length"))
       .otherwise(lit(0))
    )
    .withColumn(
      "val_fluid_loading",
      when(col("val_gross_lateral_length") > 0,
           col("val_total_fluid_loading") / col("val_gross_lateral_length"))
       .otherwise(lit(0))
    )
}

// COMMAND ----------

var dlInsert = "N/A"
var dlMsg = ""




var dfp: DataFrame  = null

var dfc: DataFrame = null

var df: DataFrame = null

try {
    
    val dfProd = generateProdSummay(getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_well_prod_oper")))

    val dfStage1 = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_stg_level"))

    val dfStage = calculateMetrics(getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_stg_level")))

    val dfDimWellbore = transformWellbore(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_wellbore_un")))

    val dfdimWellHole = transformWell(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hole_unc")))

    val dfEffect = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_well_prod_oper_effective"))

    val dfPerf = transformdfperf(getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_com_perf")))
    
    dfp = dfProd

    dfp = joinProdWellbore(dfp, dfDimWellbore)

    dfc = dfStage

    dfc = join(dfc, dfDimWellbore, "cod_uwbi", "left").withColumn("id_vers_well_hole", coalesce(col("id_vers_well_hole"), lit("-99")))
                                                      .withColumn("cod_vers_wellbore", coalesce(col("cod_vers_wellbore"), lit("-99")))
                                                      .drop("id_well")
                                    

    df = joinProdAndComp(dfc,dfp)

    df = join(df, dfdimWellHole, "id_vers_well_hole", "left")
    
    df = addEffectiveAccumulatedColumnsCond(df, dfEffect)

    df = addEffectiveAccumulatedColumnsGas(df, dfEffect)

    df = addEffectiveAccumulatedColumnsOil(df, dfEffect)

    df = addEffectiveAccumulatedColumnsWater(df, dfEffect)

    df = Joinperfwellb(df, dfPerf)

    df= calculateLoadings(df)

    df = df.select(orderFields.map(col): _*)

    df = df
    
    df = brounddf(df)

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

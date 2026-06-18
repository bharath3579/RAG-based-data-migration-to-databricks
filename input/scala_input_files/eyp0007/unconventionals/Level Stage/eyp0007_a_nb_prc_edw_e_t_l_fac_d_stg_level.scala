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
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame


val order = Seq("cod_vers_wellbore", "cod_vers_well_string", "cod_vers_well", "cod_uwbi","cod_api12", "cod_uwbi_num_stage", "num_stage","val_interval_top","val_interval_base",
"val_stage_mid_perf_depth", "val_stage_mid_perf_depth_tvd","val_prefrac_isip", "val_completed_length","val_prefrac_isip_gradient","val_postfrac_isip","val_postfrac_isip_gradient",
"avg_treating_pressure","max_treating_pressure","avg_pump_rate","max_pump_rate","val_total_proppant","val_stage_clean_fluid","val_stage_slurry_volume",
"val_stage_acid_volume","val_slickwater","val_fresh_water","val_flowback_water","val_linear_gel","val_stage_crosslnk_gel_volume",
"val_stg_frac_flowback_water","val_stage_100mesh_sand_volume","val_stage_40_70_sand_volume","val_stage_30_50_sand_volume","val_stage_20_40_sand_volume",
"fec_com_date","des_stg_geost_interval", "val_flag_anomaly_isip_gradient","val_flag_anomaly_sand_proportion", "val_flag_anomaly_fluid_proportion", 
"num_perforations", "val_latitude", "val_longitude", "val_perf_base", "val_perf_top")

// Renombramientos para dfFinal
def transformationdf(df: DataFrame): DataFrame = {
  var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("cod_vers_wellbore_csf", "cod_vers_wellbore")
                 .withColumnRenamed("num_stage_csf", "num_stage")
                 .withColumnRenamed("val_perfrac_isip", "val_prefrac_isip")

            
  return dfTrn
}

def addGradientColumns(df: DataFrame): DataFrame = {
  df.withColumn("val_prefrac_isip_gradient", 
    (col("val_perfrac_isip") / col("val_stage_mid_perf_depth_tvd")) + 0.433)
    .withColumn("val_postfrac_isip_gradient", 
      (col("val_postfrac_isip") / col("val_stage_mid_perf_depth_tvd")) + 0.433)
}

def addextracolumn(df: DataFrame): DataFrame = {
  df.withColumn("des_stg_geost_interval", lit(null).cast(DoubleType))
    .withColumn("val_flag_anomaly_sand_proportion", lit(null).cast(IntegerType))
    .withColumn("val_flag_anomaly_fluid_proportion", lit(null).cast(IntegerType))
}

// COMMAND ----------

val dfwellbore: DataFrame = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_wellbore_un"))
val dfwellboreselected = dfwellbore.select("id_vers_rel","cod_api12","cod_uwbi", "id_vers_well", "id_vers_well_hole", "des_business_unit")
val dfCSA: DataFrame = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_comp_stg_actual"))
display(dfCSA)
val dfCSF: DataFrame = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_comp_stg_final"))
val dfwellboreselect= dfwellboreselected.withColumnRenamed("id_vers_rel", "id_vers_rel_well")
display(dfCSF)

val dfCSAfiltered = dfCSA.columns.foldLeft(dfCSA) { (tempDf, colName) =>
  tempDf.withColumnRenamed(colName, s"${colName}_csa")
}

val dfCSFSelected = dfCSF.columns.foldLeft(dfCSF) { (tempDf, colName) =>
  tempDf.withColumnRenamed(colName, s"${colName}_csf")
}

val dfCSACSF = dfCSFSelected
.join(
  dfCSAfiltered,
  dfCSFSelected("cod_uwbi_csf") === dfCSAfiltered("cod_uwbi_csa") && 
  dfCSFSelected("num_stage_csf") === dfCSAfiltered("num_stage_csa"),
  "left"
)

val CAFCondition = dfCSACSF
.withColumn("val_perfrac_isip", when(col("val_perfrac_isip_csa").isNotNull, col("val_perfrac_isip_csa")).otherwise(col("val_prefrac_isip_csf")))
.withColumn("val_postfrac_isip", when(col("val_postfrac_isip_csa").isNotNull, col("val_postfrac_isip_csa")).otherwise(col("val_postfrac_isip_csf")))
.withColumn("avg_treating_pressure", when(col("avg_treating_pressure_csa").isNotNull, col("avg_treating_pressure_csa")).otherwise(col("avg_treating_pressure_csf")))
.withColumn("max_treating_pressure", when(col("max_treating_pressure_csa").isNotNull, col("max_treating_pressure_csa")).otherwise(col("max_treating_pressure_csf")))
.withColumn("avg_pump_rate", when(col("avg_pump_rate_csa").isNotNull, col("avg_pump_rate_csa")).otherwise(col("avg_pump_rate_csf")))
.withColumn("avg_pump_rate", when(col("avg_pump_rate_csa").isNotNull, col("avg_pump_rate_csa")).otherwise(col("avg_pump_rate_csf")))
.withColumn("max_pump_rate", when(col("max_pump_rate_csa").isNotNull, col("max_pump_rate_csa")).otherwise(col("max_pump_rate_csf")))
.withColumn("val_total_proppant", when(col("val_total_proppant_csf").isNotNull, col("val_total_proppant_csf")).otherwise(col("val_total_proppant_csa")))
.withColumn("val_stage_clean_fluid", when(col("val_stage_clean_fluid_csf").isNotNull, col("val_stage_clean_fluid_csf")).otherwise(col("val_stage_clean_fluid_csa")))
.withColumn("val_stage_slurry_volume", when(col("val_stage_slurry_volume_csf").isNotNull, col("val_stage_slurry_volume_csf")).otherwise(col("val_stage_slurry_volume_csa")))
.withColumn("cod_uwbi_num_stage", when(col("cod_uwbi_num_stage_csf").isNotNull, col("cod_uwbi_num_stage_csf")).otherwise(col("cod_uwbi_num_stage_csa")))
.withColumnRenamed("val_stage_slickwater_volume_csa", "val_slickwater")
.withColumnRenamed("val_stage_linear_gel_volume_csa", "val_linear_gel")
.withColumnRenamed("val_stage_mid_perf_depth_csf", "val_stage_mid_perf_depth")
.withColumnRenamed("val_stage_mid_perf_depth_tvd_csf", "val_stage_mid_perf_depth_tvd")
.withColumnRenamed("val_flowback_water_csf", "val_flowback_water")
.withColumnRenamed("val_stg_frac_flowback_water_csf", "val_stg_frac_flowback_water")
.withColumnRenamed("val_interval_top_csf", "val_interval_top")
.withColumnRenamed("val_interval_base_csf", "val_interval_base")
.withColumnRenamed("val_stage_acid_volume_csf", "val_stage_acid_volume")
.withColumnRenamed("val_fresh_water_csf", "val_fresh_water")
.withColumnRenamed("val_stage_100mesh_sand_volume_csf", "val_stage_100mesh_sand_volume")
.withColumnRenamed("val_stage_40_70_sand_volume_csf", "val_stage_40_70_sand_volume")
.withColumnRenamed("val_stage_30_50_sand_volume_csf", "val_stage_30_50_sand_volume")
.withColumnRenamed("val_stage_20_40_sand_volume_csf", "val_stage_20_40_sand_volume")
.withColumnRenamed("val_stage_crosslnk_gel_volume_csa", "val_stage_crosslnk_gel_volume")
.withColumnRenamed("fec_time_start_csf", "fec_com_date")
.withColumnRenamed("val_completed_length_csf", "val_completed_length")
.withColumnRenamed("val_latitude_csf", "val_latitude")
.withColumnRenamed("val_longitude_csf", "val_longitude")

val dfCSACSFwell = CAFCondition
  .join(
    dfwellboreselect,
    CAFCondition("cod_vers_wellbore_csf") === dfwellboreselect("id_vers_rel_well"),
    "left"
  ).withColumn("cod_vers_well_string", coalesce(dfwellbore("id_vers_well"), lit("-99"))).withColumn("cod_vers_well", coalesce(dfwellbore("id_vers_well_hole"), lit("-99")))

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

def addFlagAnomalyISIPGradient(df: DataFrame): DataFrame = {
  df.withColumn(
    "val_flag_anomaly_isip_gradient",
    when(
      (col("des_business_unit") === "Marcellus") && (col("val_postfrac_isip_gradient") > 1.3), 1
    ).when(
      (col("des_business_unit") === "EagleFord") && (col("val_postfrac_isip_gradient") > 1.12), 1
    ).otherwise(0)
  )
}


// COMMAND ----------


def joinWithCompPerfo(df: DataFrame, comp_perfo: DataFrame): DataFrame = {

  val dfJoined = df.join(comp_perfo, Seq("cod_vers_wellbore", "num_stage"), "left")
  
  dfJoined
}

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

// UDF para redondear a múltiplos de 5 (ej.: 10, 15, 20, etc.)
val roundToNearest5 = udf((x: Double) => math.round(x / 5.0) * 5)

def joinAndAddGeoInterval(df1: DataFrame, dfGeo: DataFrame): DataFrame = {
  // En df1: calcular la clave (PK) a partir de val_stage_mid_perf_depth
  val df1WithPk = df1.withColumn("pk", roundToNearest5(col("val_stage_mid_perf_depth")))
  
  // En dfGeo: calcular la PK a partir de val_md (asegurando que sea Double)
  val dfGeoWithPk = dfGeo.withColumn("pk", roundToNearest5(col("val_md").cast(DoubleType)))
  
  // Realizamos el join entre df1 y dfGeo usando la PK, seleccionando las columnas necesarias de dfGeo
  val joined = df1WithPk.join(
    dfGeoWithPk.select("pk", "des_geosteering_log", "val_md"),
    Seq("pk"),
    "left"
  )
  
  // Calculamos la diferencia entre el valor original de df1 y el val_md de dfGeo
  val joinedWithDiff = joined.withColumn("diff", abs(col("val_stage_mid_perf_depth") - col("val_md")))
  
  // Definimos una ventana particionada por pk y ordenada por diff
  val w = Window.partitionBy("pk").orderBy(col("diff"))
  
  // Seleccionamos, para cada PK, la fila con menor diferencia y renombramos des_geosteering_log a tmp_geo_interval
  val bestMatch = joinedWithDiff.withColumn("rn", row_number().over(w))
    .filter(col("rn") === 1)
    .select(
      col("pk"),
      col("des_geosteering_log").alias("tmp_geo_interval")
    )
  
  // Eliminamos la columna des_stg_geost_interval de df1 para evitar ambigüedad
  val df1Cleaned = df1WithPk.drop("des_stg_geost_interval")
  
  // Unimos df1 limpio con bestMatch para incorporar el valor en la columna des_stg_geost_interval
  val dfFinal = df1Cleaned.alias("a")
    .join(bestMatch.alias("b"), Seq("pk"), "left")
    .selectExpr("a.*", "b.tmp_geo_interval as des_stg_geost_interval")
  
  dfFinal
}


// COMMAND ----------

def stageFlagsand(df1: DataFrame, df2: DataFrame): DataFrame = {
  // Filtrar y seleccionar las columnas necesarias de df2 (plan)
  val dfplan = df2.select("cod_uwbi","num_stage","val_stage_100mesh_sand_volume","val_stage_40_70_sand_volume",
                          "val_stage_30_50_sand_volume","val_stage_20_40_sand_volume","val_stage_slickwater_volume")
    .filter(col("cod_uwbi").isNotNull && col("cod_uwbi") =!= "-99")

  // Renombrar las columnas de dfCSA para agregar el sufijo '_plan'
  val dfplanadd = dfplan.columns.foldLeft(dfplan) { (tempDf, colName) =>
    tempDf.withColumnRenamed(colName, s"${colName}_plan")
  }
 
  // Realizar el left join entre df1 y df2 (dfplanadd)
  val dfJoin = df1.join(dfplanadd, df1("cod_uwbi") === dfplanadd("cod_uwbi_plan") && df1("num_stage") === dfplanadd("num_stage_plan"), "left")
    .drop("cod_uwbi_plan", "num_stage_plan")
 
  // Usar coalesce para reemplazar valores nulos por 0 y luego calcular las sumas
  val suma_vol_real = coalesce(col("val_stage_100mesh_sand_volume"), lit(0)) + 
                      coalesce(col("val_stage_40_70_sand_volume"), lit(0)) + 
                      coalesce(col("val_stage_30_50_sand_volume"), lit(0)) + 
                      coalesce(col("val_stage_20_40_sand_volume"), lit(0))
 
  val suma_vol_plan = coalesce(col("val_stage_100mesh_sand_volume_plan"), lit(0)) + 
                      coalesce(col("val_stage_40_70_sand_volume_plan"), lit(0)) + 
                      coalesce(col("val_stage_30_50_sand_volume_plan"), lit(0)) + 
                      coalesce(col("val_stage_20_40_sand_volume_plan"), lit(0))
 
  // Crear la columna 'val_flag_anomaly_sand_proportion' con base en la condición
  val dfConAnomalias = dfJoin.withColumn("val_flag_anomaly_sand_proportion", 
                                         when(suma_vol_real < suma_vol_plan * 0.9, lit(1))
                                         .otherwise(lit(0)))
 
  // Devolver el DataFrame con la columna añadida
  dfConAnomalias
}
 
 
 
def stageFlagFluidProportion(df: DataFrame): DataFrame = {
  // Usar coalesce para reemplazar valores nulos por 0 y luego calcular la comparación
  val dfConAnomalias = df.withColumn("val_flag_anomaly_fluid_proportion",
    when(coalesce(col("val_slickwater"), lit(0)) > coalesce(col("val_stage_slickwater_volume_plan"), lit(0)) * 1.15, lit(1))
      .otherwise(lit(0)))
 
  // Devolver el DataFrame con la columna añadida
  dfConAnomalias
}

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

// UDF para redondear al múltiplo de 5 más cercano (ej.: 10, 15, 20, etc.)
val roundToNearest5 = udf((x: Double) => math.round(x / 5.0) * 5)

/**
  * Función que une los DataFrames de Stage (dfStg), Geosteering (dfGeo) y Wellbore (dfWb)
  * para agregar la columna des_stg_geost_interval (derivada de des_geosteering_log) al resultado.
  *
  * - En dfStg se calcula pk_val_stage redondeando val_stage_mid_perf_depth al múltiplo de 5.
  * - En dfGeo se calcula pk_val_md redondeando val_md al entero más cercano.
  * - Se genera además una clave compuesta en cada uno (pk_stage_level_full y pk_geosteering_full).
  * - Se utiliza dfStg como tabla base; se hace un LEFT JOIN con dfGeo usando:
  *       • cod_vers_wellbore, y
  *       • pk_val_stage = pk_val_md.
  * - Para evitar la ambigüedad en cod_vers_wellbore se elimina la columna duplicada de dfGeo.
  * - Se hace un segundo LEFT JOIN con dfWb para traer, por ejemplo, des_business_unit.
  * - Finalmente se seleccionan las columnas clave y se ordena el resultado.
  */
def joinStageGeosteeringWellbore(dfStg: DataFrame, dfGeo: DataFrame, dfWb: DataFrame): DataFrame = {
  
  // Procesamos dfStg: calculamos pk_val_stage y la clave compuesta pk_stage_level_full;
  // renombramos cod_uwbi a stg_uwbi
  val stg = dfStg
    .withColumn("pk_val_stage", roundToNearest5(col("val_stage_mid_perf_depth")))
    .withColumn("pk_stage_level_full", concat(col("cod_vers_wellbore"), lit("_"), col("pk_val_stage")))
    .withColumnRenamed("cod_uwbi", "stg_uwbi")
  
  // Procesamos dfGeo: calculamos pk_val_md y la clave compuesta pk_geosteering_full;
  // renombramos uwbi a geo_uwbi
  val geo = dfGeo
    .withColumn("pk_val_md", round(col("val_md"), 0).cast(IntegerType))
    .withColumn("pk_geosteering_full", concat(col("cod_vers_wellbore"), lit("_"), col("pk_val_md")))
    .withColumnRenamed("uwbi", "geo_uwbi")
  
  // Procesamos dfWb: seleccionamos las columnas necesarias
  val wb = dfWb.select(
    col("id_vers_rel"),
    col("des_business_unit")
  )
  
  // Realizamos el join:
  // 1. Usamos dfStg (alias "s") como tabla base.
  // 2. Hacemos LEFT JOIN con dfGeo (alias "g") utilizando cod_vers_wellbore y la igualdad de claves.
  // 3. Para evitar ambigüedad, eliminamos la columna cod_vers_wellbore proveniente de dfGeo.
  // 4. Luego, LEFT JOIN con dfWb (alias "w") usando s.cod_vers_wellbore = w.id_vers_rel.
  val joined = stg.alias("s")
    .join(
      geo.alias("g"),
      stg("cod_vers_wellbore") === geo("cod_vers_wellbore") &&
      stg("pk_val_stage") === geo("pk_val_md"),
      "left"
    )
    .drop(geo("cod_vers_wellbore"))  // Eliminamos la columna duplicada para evitar ambigüedad
    .join(
      wb.alias("w"),
      stg("cod_vers_wellbore") === wb("id_vers_rel"),
      "left"
    )
  
  // Seleccionamos las columnas clave y renombramos des_geosteering_log a des_stg_geost_interval
  val finalDF = joined.select(
    col("s.stg_uwbi"),
    col("s.num_stage"),
    col("s.val_stage_mid_perf_depth"),
    col("s.pk_stage_level_full"),
    col("g.des_geosteering_log").alias("des_stg_geost_interval"),
    col("g.pk_geosteering_full"),
    col("w.des_business_unit")
  )
  
  // Ordenamos el resultado: stg_uwbi descendente y num_stage ascendente
  finalDF.orderBy(col("s.stg_uwbi").desc, col("s.num_stage").asc).dropDuplicates()
}


// COMMAND ----------

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {   println("1")
        val FinalDF = addGradientColumns(dfCSACSFwell)
        println("2")
        val FinalDFtrans = transformationdf(FinalDF)
        println("3")
        val dfwithallcolum = addextracolumn(FinalDFtrans)
        println("4")

        val dfCompPerfo = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_com_perf"))

        val dfGeo = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_geosteering"))

        val dfNumPerfo = getDataFromSource(sourcesFinal("am_eyp0007_tb_fac_d_stg_perf_inter"))
        println("5")

        df = addFlagAnomalyISIPGradient(dfwithallcolum)
        println("6")
        
        //df = addFlagAnomalyPumpingRate(df)

        df = joinWithCompPerfo(df, dfNumPerfo)
        println("7")

        df = joinAndAddGeoInterval(df, dfGeo)

        println("8")

        df = stageFlagsand(df, getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_comp_stg_plan")))

        df = stageFlagFluidProportion(df)
        println("9")

        val df2 = joinStageGeosteeringWellbore(df, dfGeo, dfwellbore)

        df = df.alias("df")
          .join(
            df2.alias("df2"), 
            col("df.cod_uwbi") === col("df2.stg_uwbi") && col("df.num_stage") === col("df2.num_stage"),
            "left"
          )
          .select(
            // Se seleccionan todas las columnas de df, reemplazando la columna des_stg_geost_interval por la de df2
            df.columns.filterNot(_ == "des_stg_geost_interval").map(c => col(s"df.$c")) :+ 
            col("df2.des_stg_geost_interval").alias("des_stg_geost_interval"): _*)
println("10")
        df = df.select(order.map(col): _*)
        println("11")
        
              
        df = df.withColumn("num_perforations", col("num_perforations").cast("Int"))
        //df.printSchema()
        df = applyDDLFromTableToDF(df,getObject((target_object).toString))
    writeWrapper(sourceDF = df, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

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

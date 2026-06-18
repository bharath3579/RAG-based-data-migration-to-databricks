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

def transformationWells (df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn.select("wells_uuid", "api")
               .withColumnRenamed("api","cod_api")
               .withColumn("cod_api_fix", regexp_replace(col("cod_api"), "[^0-9]", ""))
               .withColumn("cod_api12", when(length(col("cod_api_fix")) === 10, concat(col("cod_api_fix"), lit("00")))
               .otherwise(substring(col("cod_api_fix"),1,12 )))
               .select("wells_uuid", "cod_api", "cod_api12")
               .dropDuplicates("wells_uuid", "cod_api12")

  return dfTrn

}

def transformationDimWellbore (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.select("fec_start_date_rel", "id_vers_rel", "cod_uwbi", "cod_api12", "id_vers_well", "id_vers_well_hole","cod_uwi")

    return dfTrn
}

/* val dfDimWellboreF = transformationDimWellbore(getDataFromSource(sourcesFinal.apply(2)))

val ge = transformationWells(dfStar3)

val joinedDF2 = dfHor3.join(ge, Seq("wells_uuid"), "inner")
display(joinedDF2)*/


def joinWellbore (dfL: DataFrame, dfR: DataFrame): DataFrame = {

    val  windowFilter = Window.partitionBy("wells_uuid", "md_val").orderBy(desc("fec_start_date_rel"))

    var dfTrn: DataFrame = null

    dfTrn = dfL.join(dfR, Seq("cod_api12"), "left").withColumn("cod_vers_wellbore", coalesce(dfR("id_vers_rel"), lit("-99"))).withColumn("cod_vers_well_string", coalesce(dfR("id_vers_well"), lit("-99"))).withColumn("cod_vers_well", coalesce(dfR("id_vers_well_hole"), lit("-99")))
    val windowDF = dfTrn.withColumn("row_number",row_number().over(windowFilter)).filter("row_number == 1")

    return windowDF
}
/*
val dfdf = joinWellbore(joinedDF2, dfDimWellboreF)
display(dfdf)*/



// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

/*import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
/*
val spark = SparkSession.builder()
  .appName("TVD Calculation")
  .getOrCreate()

import spark.implicits._*/

// Tu DataFrame de entrada
val inputDF = Seq(
  ("fc976474-8478-4e2a-a169-c3c43a003ded", 0, 0, 0),
  ("fc976474-8478-4e2a-a169-c3c43a003ded", 17373, 25, 1890),
  ("fc976474-8478-4e2a-a169-c3c43a003ded", 14892, 35, 2800)
).toDF("wells_uuid", "azim_val", "incl_val", "md_val")

// Ventana para realizar cálculos por cada wellbore
val wellboreWindow = Window.partitionBy("wells_uuid").orderBy("md_val")

// Cálculo de delta_md
val withDeltaMD = inputDF.withColumn("delta_md", 
  $"md_val" - lag("md_val", 1, 0).over(wellboreWindow)
)

// Conversión de inclinación a radianes
val withInclRadians = withDeltaMD.withColumn("incl_rad", radians($"incl_val"))

// Cálculo de delta_tvd
val withDeltaTVD = withInclRadians.withColumn("delta_tvd", 
  $"delta_md" * cos($"incl_rad")
)

// Cálculo acumulativo del TVD
val resultDF = withDeltaTVD.withColumn("tvd", 
  sum("delta_tvd").over(wellboreWindow)
)

// Mostrar el resultado
resultDF.show(truncate = false)*/
def calculateTVD(df: DataFrame): DataFrame = {
  // Limpiar y validar datos
  val cleanedDF = df
    .filter($"md_val".isNotNull && $"incl_val".isNotNull) // Filtrar nulos
    .withColumn("md_val", $"md_val".cast("double"))      // Convertir a numérico
    .withColumn("incl_val", $"incl_val".cast("double"))  // Convertir a numérico
    .withColumn("azim_val", $"azim_val".cast("double"))  // Convertir a numérico
    .filter($"md_val" >= 0)                              // Filtrar valores negativos en md_val

  // Ventana para particionar por cada wellbore y ordenar por MD
  val wellboreWindow = Window.partitionBy("wells_uuid").orderBy("md_val")

  // Agregar delta_md
  val withDeltaMD = cleanedDF.withColumn("delta_md", 
    $"md_val" - lag("md_val", 1, 0).over(wellboreWindow)
  )

  // Convertir inclinación a radianes
  val withInclRadians = withDeltaMD.withColumn("incl_rad", radians($"incl_val"))

  // Calcular delta_tvd
  val withDeltaTVD = withInclRadians.withColumn("delta_tvd", 
    $"delta_md" * cos($"incl_rad")
  )

  // Calcular el TVD acumulativo
  val resultDF = withDeltaTVD.withColumn("val_tvd", 
    sum("delta_tvd").over(wellboreWindow)
  )

  // Retornar el DataFrame con las columnas adicionales
  resultDF
}



// COMMAND ----------

def renameAndModifyColumns(df: DataFrame): DataFrame = {
  // Eliminar las columnas que no necesitas
  val dfWithoutDroppedCols = df
    .drop("cod_api12")
    .drop("wells_uuid")
    .drop("year")
    .drop("month")
    .drop("day")
    .drop("fec_start_date_rel")
    .drop("id_vers_rel")
    .drop("id_vers_well")
    .drop("id_vers_well_hole")
    .drop("cod_vers_wellbore")
    .drop("cod_vers_well_string")
    .drop("cod_vers_well")
    .drop("row_number")
    .drop("val_inclination_rad")
    .drop("val_azimuth_rad")
    .drop("prev_val_md")
    .drop("prev_inclination_rad")
    .drop("prev_azimuth_rad")
    .drop("val_dls")
    .drop("val_curvature_factor")
    .drop("delta_val_md")
    .drop("delta_val_tvd")

  // Renombrar las columnas necesarias
  val dfWithRenamedCols = dfWithoutDroppedCols
    .withColumnRenamed("azim_val", "val_station_az").withColumn("val_station_az", col("val_station_az").cast("double"))
    .withColumnRenamed("incl_val", "val_station_dev").withColumn("val_station_dev", col("val_station_dev").cast("double"))
    .withColumnRenamed("md_val", "val_station_md").withColumn("val_station_md", col("val_station_md").cast("double"))
    .withColumnRenamed("val_tvd", "val_station_tvd").withColumn("val_station_tvd", col("val_station_tvd").cast("double"))

  // Añadir las nuevas columnas con valores nulos
  val dfWithAddedCols = dfWithRenamedCols
    .withColumn("WELL_ID", lit(null).cast(StringType))
    .withColumn("WELLBORE_ID", lit(null).cast(StringType))
    .withColumn("val_geo_offset_east_bh", lit(null).cast(DoubleType))
    .withColumn("val_geo_offset_north_bh", lit(null).cast(DoubleType))
    .withColumn("val_geo_latitude_bh", lit(null).cast(DoubleType))
    .withColumn("val_geo_longitude_bh", lit(null).cast(DoubleType))
    .withColumn("fec_date_td_reached", lit(null).cast(TimestampType))
    .withColumn("UPDATE_DATE", lit(null).cast(TimestampType))
    .withColumn("num_is_certified", lit(0).cast(IntegerType))
    .withColumn("val_station_az_grid", lit(null).cast("double"))

  // Reordenar las columnas en el orden especificado
  val finalColumnOrder = Seq(
    "WELL_ID", "WELLBORE_ID", "val_station_md", "val_station_tvd", "val_station_dev", "val_station_az", 
    "cod_uwbi", "val_geo_offset_east_bh", "val_geo_offset_north_bh", "val_geo_latitude_bh", "val_geo_longitude_bh", 
    "fec_date_td_reached", "UPDATE_DATE", "cod_uwi", "num_is_certified","val_station_az_grid"
  )
  
  // Seleccionar las columnas en el orden especificado
  val dfWithOrderedColumns = dfWithAddedCols.select(finalColumnOrder.map(col): _*)

  // Devolver el DataFrame modificado con el nuevo orden de columnas
  dfWithOrderedColumns
}

// COMMAND ----------

def transformationDimWell (df: DataFrame): DataFrame = {
        var dfTrn: DataFrame = df
        dfTrn = dfTrn.filter(col("des_state_or_province").isin("PENNSYLVANIA", "TEXAS", "NEW YORK"))
                     .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
                     .select("cod_uwi", "val_preferred_easting", "val_preferred_northing", "val_datum_elevation")
                     .dropDuplicates
                    

    return dfTrn

} 
def join (dfL: DataFrame, dfR: DataFrame, key: String): DataFrame = {

    var dfTrn: DataFrame = null
    dfTrn = dfL.join(dfR, Seq(key), "inner")

return dfTrn
}


// COMMAND ----------

// DBTITLE 1,MIGRATED
import org.apache.spark.sql.{DataFrame, Row, Encoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer
import java.lang.Math

def calculateXYIteratively(df: DataFrame): DataFrame = {

  val schema = StructType(df.schema.fields ++ Array(
    StructField("x_interpolado", DoubleType, nullable = true),
    StructField("y_interpolado", DoubleType, nullable = true)
  ))

  implicit val rowEncoder: Encoder[Row] = ExpressionEncoder(schema)

  df
    // ensure all rows of a well land together (but DO NOT rely on partition boundaries)
    .repartition(col("cod_uwbi"))
    // enforce correct order inside each well
    .sortWithinPartitions("cod_uwbi", "val_station_md")
    .mapPartitions { rows =>

      val result = ListBuffer[Row]()

      var lastWell: String = null

      var prevEasting: java.lang.Double = null
      var prevNorthing: java.lang.Double = null
      var prevMd: Double = 0.0
      var prevDev: Double = 0.0
      var prevAzimuth: Double = 0.0

      // if initial coordinates are null → entire well is null
      var wellHasValidStart = true

      rows.foreach { row =>

        val well = row.getAs[String]("cod_uwbi")

        val md = row.getAs[Double]("val_station_md")
        val dev = row.getAs[Double]("val_station_dev")
        val az = row.getAs[Double]("val_station_az")

        // detect new well boundary
        if (lastWell == null || well != lastWell) {

          lastWell = well

          prevEasting = row.getAs[java.lang.Double]("val_preferred_easting")
          prevNorthing = row.getAs[java.lang.Double]("val_preferred_northing")

          prevMd = md
          prevDev = dev
          prevAzimuth = az

          wellHasValidStart = !(prevEasting == null || prevNorthing == null)

          if (wellHasValidStart) {
            result += Row.fromSeq(row.toSeq ++ Seq(prevEasting, prevNorthing))
          } else {
            result += Row.fromSeq(row.toSeq ++ Seq(null, null))
          }

        } else if (!wellHasValidStart) {

          // Synapse behavior: if first coords are null, ALL rows are null
          result += Row.fromSeq(row.toSeq ++ Seq(null, null))

        } else {

          val devRad = Math.toRadians(dev)
          val azRad = Math.toRadians(az)
          val prevDevRad = Math.toRadians(prevDev)
          val prevAzRad = Math.toRadians(prevAzimuth)

          val cosDogLeg =
            Math.cos(devRad - prevDevRad) -
              Math.sin(prevDevRad) * Math.sin(devRad) *
                (1 - Math.cos(azRad - prevAzRad))

          val clippedCos = Math.max(-1.0, Math.min(1.0, cosDogLeg))
          val dogLeg = Math.acos(clippedCos)

          val dogLegFactor =
            if (dogLeg != 0.0) (2.0 / dogLeg) * Math.tan(dogLeg / 2.0)
            else 1.0

          val deltaMd = md - prevMd

          val deltaEasting =
            (deltaMd / 2.0) *
              (Math.sin(prevDevRad) * Math.sin(prevAzRad) +
               Math.sin(devRad) * Math.sin(azRad)) *
              dogLegFactor

          val deltaNorthing =
            (deltaMd / 2.0) *
              (Math.sin(prevDevRad) * Math.cos(prevAzRad) +
               Math.sin(devRad) * Math.cos(azRad)) *
              dogLegFactor

          prevEasting += deltaEasting
          prevNorthing += deltaNorthing

          prevMd = md
          prevDev = dev
          prevAzimuth = az

          result += Row.fromSeq(row.toSeq ++ Seq(prevEasting, prevNorthing))
        }
      }

      result.iterator
    }
}


// COMMAND ----------

var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {
    val dfDimWellbore = transformationDimWellbore(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_wellbore_un")))
    val welldf = transformationWells(getDataFromSource(sourcesFinal("`aa000935|glb|well`")))
    val trajectorydf = getDataFromSource(sourcesFinal("`aa000935|glb|well_trajectory`"))
    val dfDimWell = transformationDimWell(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hole_unc")))
    
    val joinedDF = trajectorydf.join(welldf, Seq("wells_uuid"), "inner")

    df = joinWellbore(joinedDF, dfDimWellbore)

    df = calculateTVD(df)

    val filteredDF = df

    df = renameAndModifyColumns(df)

    df = join(df, dfDimWell, "cod_uwi")

    df = calculateXYIteratively(df)

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

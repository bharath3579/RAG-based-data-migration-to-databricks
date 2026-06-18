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
def transformationDefSurvStat (df: DataFrame): DataFrame = {
    var dfTrn: DataFrame = df
    dfTrn = dfTrn.withColumnRenamed("MD", "val_station_md")
                .withColumnRenamed("TVD", "val_station_tvd")
                .withColumnRenamed("INCLINATION", "val_station_dev")
                .withColumnRenamed("AZIMUTH", "val_station_az")
                .select("DEF_SURVEY_HEADER_ID","WELLBORE_ID", "val_station_md", "val_station_tvd", "val_station_dev", "val_station_az")

return dfTrn
}

def transformationWellboreEDM (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df
    dfTrn = dfTrn.withColumnRenamed("GEO_OFFSET_EAST_BH", "val_geo_offset_east_bh")
                .withColumnRenamed("GEO_OFFSET_NORTH_BH", "val_geo_offset_north_bh")
                .withColumnRenamed("GEO_LATITUDE_BH", "val_geo_latitude_bh")
                .withColumnRenamed("GEO_LONGITUDE_BH", "val_geo_longitude_bh")
                .withColumnRenamed("DATE_TD_REACHED", "fec_date_td_reached")
                .withColumnRenamed("WELLBORE_UWI", "cod_uwbi")
                .select("cod_uwbi", "val_geo_offset_east_bh", "val_geo_offset_north_bh", "val_geo_latitude_bh", "val_geo_longitude_bh", "fec_date_td_reached", "WELLBORE_ID", "WELL_ID","UPDATE_DATE")

    return dfTrn
} 

def transformationWellEDM (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df
    dfTrn = dfTrn.withColumnRenamed("REPSOL_WELL_UWI", "cod_uwi")
                .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
                .select("cod_uwi", "WELL_ID", "CONVERGENCE")
                .dropDuplicates("cod_uwi")

    return dfTrn
} 

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

// MAGIC %md
// MAGIC # Función de interpolación de X e Y

// COMMAND ----------

import org.apache.spark.sql.{Encoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import scala.collection.mutable.ListBuffer
import java.lang.Math

def calculateXYIteratively(df: DataFrame): DataFrame = {
  val schema = StructType(df.schema.fields ++ Array(
    StructField("x_interpolado", DoubleType, nullable = true),
    StructField("y_interpolado", DoubleType, nullable = true)
  ))

  implicit val rowEncoder: Encoder[Row] = ExpressionEncoder(schema)

  val dfWithXY =
    df
      .repartition(col("WELLBORE_ID"))
      .mapPartitions { iter =>

        val groupedRows = iter.toSeq.groupBy(_.getAs[String]("WELLBORE_ID"))

        groupedRows.valuesIterator.flatMap { wellboreRows =>

          // Ordenar las filas por val_station_md
          val sortedRows = wellboreRows.sortBy(_.getAs[Double]("val_station_md"))
          val resultRows = ListBuffer[Row]()

          // Obtener las coordenadas iniciales
          val initialRow = sortedRows.head
          val initialEasting = initialRow.getAs[java.lang.Double]("val_preferred_easting")
          val initialNorthing = initialRow.getAs[java.lang.Double]("val_preferred_northing")

          // Verificar si las coordenadas iniciales son nulas
          if (initialEasting == null || initialNorthing == null) {
            // Si son nulas, asignar nulo a x_interpolado e y_interpolado en todos los registros
            for (row <- sortedRows) {
              val newRow = Row.fromSeq(row.toSeq ++ Seq(null, null))
              resultRows += newRow
            }
          } else {
            // Si las coordenadas iniciales no son nulas, proceder con el cálculo
            var prevEasting = initialEasting
            var prevNorthing = initialNorthing
            var prevMd = initialRow.getAs[Double]("val_station_md")
            var prevDev = initialRow.getAs[Double]("val_station_dev")
            var prevAzimuth = initialRow.getAs[Double]("val_station_az_grid")

            for ((row, index) <- sortedRows.zipWithIndex) {
              val currentMd = row.getAs[Double]("val_station_md")
              val currentDev = row.getAs[Double]("val_station_dev")
              val currentAzimuth = row.getAs[Double]("val_station_az_grid")

              if (index == 0) {
                // Primera MD, usar las coordenadas iniciales
                val newRow = Row.fromSeq(row.toSeq ++ Seq(prevEasting, prevNorthing))
                resultRows += newRow
              } else {
                // Calcular x_interpolado e y_interpolado
                val devRad = Math.toRadians(currentDev)
                val aziRad = Math.toRadians(currentAzimuth)
                val devPrevRad = Math.toRadians(prevDev)
                val aziPrevRad = Math.toRadians(prevAzimuth)

                // Calcular Dogleg Severity
                val cosDogLegSeverity = Math.cos(devRad - devPrevRad) - Math.sin(devPrevRad) * Math.sin(devRad) * (1 - Math.cos(aziRad - aziPrevRad))
                val cosDogLegSeverityClipped = Math.max(-1.0, Math.min(1.0, cosDogLegSeverity))
                val dogLegSeverity = Math.acos(cosDogLegSeverityClipped)
                val dogLegFactor = if (dogLegSeverity != 0.0) (2.0 / dogLegSeverity) * Math.tan(dogLegSeverity / 2.0) else 1.0

                // Calcular deltaMD
                val deltaMD = currentMd - prevMd

                // Calcular deltaEasting y deltaNorthing
                val deltaEasting = (deltaMD / 2.0) * (Math.sin(devPrevRad) * Math.sin(aziPrevRad) + Math.sin(devRad) * Math.sin(aziRad)) * dogLegFactor
                val deltaNorthing = (deltaMD / 2.0) * (Math.sin(devPrevRad) * Math.cos(aziPrevRad) + Math.sin(devRad) * Math.cos(aziRad)) * dogLegFactor

                // Calcular easting y northing actuales
                prevEasting = prevEasting + deltaEasting.toDouble
                prevNorthing = prevNorthing + deltaNorthing.toDouble

                prevMd = currentMd
                prevDev = currentDev
                prevAzimuth = currentAzimuth

                val newRow = Row.fromSeq(row.toSeq ++ Seq(prevEasting, prevNorthing))
                resultRows += newRow
              }
            }
          }

          resultRows
        }
      }

  dfWithXY
}


// COMMAND ----------

def joinsurvey(dfL: DataFrame, dfR: DataFrame, key: String): DataFrame = {
  // Realiza el join tipo left con la clave especificada
  val dfR2 = dfR.select("DEF_SURVEY_HEADER_ID", "PHASE")
  //val dfR3 = dfR2.filter(!$"PHASE".isin("PROTOTYPE", "PLAN"))
  val dfR3 = dfR2.filter(col("PHASE").contains("ACTUAL"))
  val dfTrn = dfL.join(dfR3, Seq(key), "inner")
  dfTrn
}

def updateStationMd(df:DataFrame): DataFrame = {
  df.withColumn("val_station_md", col("val_station_md") + col("val_datum_elevation")).withColumn("val_station_tvd", col("val_station_tvd") + col("val_datum_elevation"))
}


// COMMAND ----------

 def calculateAzimuthGrid(df: DataFrame): DataFrame = {
  val dfWithAzimuth = df.withColumn(
    "val_station_az_grid", 
    col("val_station_az") - coalesce(col("CONVERGENCE"), lit(0)) // Si CONVERGENCE es NULL, usa 0
  )
  
  dfWithAzimuth.drop("CONVERGENCE") // Elimina la columna original si ya no la necesitas
}

// COMMAND ----------

var df: DataFrame  = null


try {

    val dfDevSurvey2 = transformationDefSurvStat(getDataFromSource(sourcesFinal("aa001236_glb_cd_definitive_survey_station_t")))
    val dfHeadersurv = getDataFromSource(sourcesFinal("aa001236_glb_cd_definitive_survey_header_t"))

    val dfDevSurvey3 =joinsurvey(dfDevSurvey2, dfHeadersurv, "DEF_SURVEY_HEADER_ID")
    
    val dfDevSurvey = dfDevSurvey3.drop("PHASE","DEF_SURVEY_HEADER_ID")
    
    val dfWellboreEdm = transformationWellboreEDM(getDataFromSource(sourcesFinal("aa001236_glb_cd_wellbore_t")))
    val dfWellEdm = transformationWellEDM(getDataFromSource(sourcesFinal("aa001236_glb_cd_well_t")))
    val dfDimWell = transformationDimWell(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hole_unc")))

    df = dfDevSurvey

    df = join(df, dfWellboreEdm, "WELLBORE_ID")

    df = join(df, dfWellEdm, "WELL_ID")
    df = df.withColumn("num_is_certified", lit(1).cast(IntegerType))
    
    df = join(df, dfDimWell, "cod_uwi")
    df = updateStationMd(df)

    df = calculateAzimuthGrid(df)

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

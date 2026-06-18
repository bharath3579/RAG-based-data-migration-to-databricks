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
def transformationDfStimTreat (df: DataFrame): DataFrame = {

    val stimWindow = Window.partitionBy("STIM_JOB_ID").orderBy("num_stage")

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("REPSOL_STAGE_NO", "num_stage")
                .withColumnRenamed("interval_top", "val_interval_top")
                .withColumnRenamed("interval_base", "val_interval_base") //
                .withColumnRenamed("time_start", "fec_time_start")
                .withColumnRenamed("time_end", "fec_time_end")
                .withColumnRenamed("repsol_pump_duration", "val_pump_duration")
                .withColumnRenamed("int_shut_in_pressure2", "val_prefrac_static_wh_press")
                .withColumnRenamed("int_shut_in_pressure_1", "val_prefrac_isip")
                .withColumnRenamed("final_shut_in_pressure", "val_postfrac_isip")
                .withColumnRenamed("int_shut_in_pressure_3", "val_5min_isip")
                .withColumnRenamed("average_pressure", "avg_treating_pressure")
                .withColumnRenamed("max_flow_pressure", "max_treating_pressure")
                .withColumnRenamed("break_pressure", "val_breakdown_pressure")
                .withColumnRenamed("average_flow_rate", "avg_pump_rate")
                .withColumnRenamed("max_flow_rate", "max_pump_rate")
                .withColumnRenamed("vol_pad", "val_stage_acid_volume")
                .withColumnRenamed("vol_pumped", "val_stage_slurry_volume")
                .withColumnRenamed("vol_body", "val_stage_clean_fluid")
                .withColumnRenamed("proppant_used", "val_total_proppant")
                .withColumnRenamed("net_stimulated", "val_completed_length")
                .withColumn("time_previous_stage", lag("fec_time_end", 1).over(stimWindow))
                .withColumn("val_time_between_stg", (col("fec_time_start").cast("long") - col("time_previous_stage").cast("long")) / 60)
                .withColumn("val_stage_mid_perf_depth", col("val_interval_top") + ((col("val_interval_base") - col("val_interval_top"))/2))
                .withColumn("val_after_prefrac_static_wh_press", lead("val_prefrac_static_wh_press", 1).over(stimWindow))
                .withColumn("val_press_decline_btw_stg", col("val_postfrac_isip") - col("val_after_prefrac_static_wh_press"))
                .withColumn("val_press_decl_5_min_shut_in", col("val_postfrac_isip") - col("val_5min_isip"))
                .withColumn("val_rt_press_decl_5_m_shut_in", col("val_press_decl_5_min_shut_in") / 5)
                .withColumn("val_rt_press_decline_btw_stg", (col("val_postfrac_isip")/col("val_prefrac_isip"))/col("val_time_between_stg"))
        
    dfTrn = dfTrn.select("STIM_JOB_ID", "STIM_TREATMENT_ID", "num_stage", "val_interval_top", "val_interval_base", "fec_time_start", "fec_time_end", "val_pump_duration", 
                         "val_prefrac_static_wh_press", "val_prefrac_isip", "val_postfrac_isip", "val_5min_isip", "avg_treating_pressure", "max_treating_pressure", 
                         "val_breakdown_pressure", "avg_pump_rate", "max_pump_rate", "val_stage_acid_volume", "val_stage_slurry_volume", "val_stage_clean_fluid", "val_total_proppant", 
                         "val_time_between_stg", "val_stage_mid_perf_depth", "val_press_decline_btw_stg", "val_press_decl_5_min_shut_in", "val_completed_length", "val_rt_press_decl_5_m_shut_in", "val_rt_press_decline_btw_stg")
              
                
   
    return dfTrn

}

def transformationStimJob (df: DataFrame): DataFrame = {
    var dfTrn: DataFrame = df

    dfTrn = dfTrn.select("WELLBORE_ID", "STIM_JOB_ID").dropDuplicates()

return dfTrn
}

def transformationWellboreEDM (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df
    dfTrn = dfTrn.withColumnRenamed("WELLBORE_UWI", "cod_uwbi")
                .select("cod_uwbi", "WELLBORE_ID", "WELL_ID")
                .dropDuplicates()

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
                     .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
                     .select("cod_uwi","des_state_or_province")
                     .dropDuplicates
                    

    return dfTrn

} 

def joinStim(dfL: DataFrame, dfR: DataFrame, key: String): DataFrame = {
    
    var dfTrn: DataFrame = null

    dfTrn = dfL.join(dfR, Seq(key), "inner")

    return dfTrn


}

def transformationDevSurvey (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.orderBy("WELLBORE_ID", "val_station_md").groupBy("WELLBORE_ID")
                .agg(
                    collect_list("val_station_md").as("mdArray"),
                    collect_list("val_station_dev").as("incArray"),
                    collect_list("val_station_az_grid").as("aziArray"),
                    collect_list("val_station_tvd").as("tvdArray"),
                    collect_list("x_interpolado").as("xArray"),
                    collect_list("y_interpolado").as("yArray"),
                    first("val_preferred_easting").as("val_preferred_easting"),
                    first("val_preferred_northing").as("val_preferred_northing"),
                    first("val_datum_elevation").as("val_datum_elevation"))
   // dfTrn = dfTrn.drop("val_datum_elevation")
                 

    return dfTrn

}

def transformationFinalStimTreatTrn (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("interpolatedIncDeg", "val_stage_mid_perf_dev")
                 .withColumnRenamed("interpolatedAziDeg", "val_stage_mid_perf_az")
                 .withColumnRenamed("accumulatedX", "val_stage_mid_perf_x_coord")
                 .withColumnRenamed("accumulatedY", "val_stage_mid_perf_y_coord")
                 .withColumnRenamed("interpolatedTVD", "val_stage_mid_perf_depth_tvd")
                 .withColumnRenamed("tvdAbsolute", "val_stage_mid_perf_depth_abs_tvd")
                 .drop("interpolatedData", "val_datum_elevation", "md1", "inc1Deg", "azi1Deg", "mdArray", "incArray", "aziArray", "tvdArray", "xArray", "yArray", "val_preferred_easting", "val_preferred_northing", "tvd1")

    return dfTrn
}


// COMMAND ----------

// MAGIC %md
// MAGIC # Función de interpolación de azimuth, inclinación, coordenadas X e Y y TVD

// COMMAND ----------

// spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
import java.lang.Math
import org.apache.spark.sql.types._

val outputSchema = StructType(Seq(
  StructField("interpolatedIncDeg", DoubleType, nullable = true),
  StructField("interpolatedAziDeg", DoubleType, nullable = true),
  StructField("md1", DoubleType, nullable = true),
  StructField("inc1Deg", DoubleType, nullable = true),
  StructField("azi1Deg", DoubleType, nullable = true),
  StructField("tvd1", DoubleType, nullable = true),
  StructField("accumulatedX", DoubleType, nullable = true),
  StructField("accumulatedY", DoubleType, nullable = true),
  StructField("interpolatedTVD", DoubleType, nullable = true),
  StructField("tvdAbsolute", DoubleType, nullable = true) // Nuevo campo
))


// Función para interpolar ángulos considerando su naturaleza circular
def interpolateAngle(angle1Deg: java.lang.Double, angle2Deg: java.lang.Double, ratio: java.lang.Double): java.lang.Double = {
  val deltaAngle = ((angle2Deg - angle1Deg + 540) % 360) - 180 // Calcula la diferencia angular mínima
  val interpolatedAngle = (angle1Deg + ratio * deltaAngle + 360) % 360
  interpolatedAngle
}
 
val minCurvatureInterpolateWithXY = udf(
  (
    md: java.lang.Double,
    mdArray: Seq[java.lang.Double],
    incArray: Seq[java.lang.Double],
    aziArray: Seq[java.lang.Double],
    tvdArray: Seq[java.lang.Double],
    initialX: java.lang.Double,
    initialY: java.lang.Double,
    xArray: Seq[java.lang.Double],
    yArray: Seq[java.lang.Double],
    datumElevation: java.lang.Double // Nuevo parámetro
  ) => {
    // Verificar si los parámetros esenciales son nulos
    if (md == null || mdArray == null || incArray == null || aziArray == null || tvdArray == null || datumElevation == null) {
      // Devolver nulos si faltan datos esenciales
      Row(null, null, null, null, null, null, null, null, null, null)
    } else {
      // Verificar si las secuencias están vacías o tienen tamaños inconsistentes
      val n = mdArray.size
      if (n == 0 || incArray.size != n || aziArray.size != n || tvdArray.size != n) {
        // Devolver nulos si los datos no son consistentes
        Row(null, null, null, null, null, null, null, null, null, null)
      } else {
        // Verificar si podemos calcular X e Y acumuladas
        val calculateXY = initialX != null && initialY != null && xArray != null && yArray != null && xArray.size == n && yArray.size == n

        if (md <= mdArray.head) {
          // Si el MD está fuera del rango por debajo, devolver el primer valor
          val firstX = if (calculateXY) initialX else null
          val firstY = if (calculateXY) initialY else null
          val interpolatedTVD = tvdArray.head
          val tvdAbsolute = datumElevation - interpolatedTVD // Calcular TVD absoluto
          Row(
            incArray.head,         // Inclinación interpolada
            aziArray.head,         // Azimuth interpolado
            mdArray.head,          // MD anterior
            incArray.head,         // Inclinación anterior
            aziArray.head,         // Azimuth anterior
            tvdArray.head,         // TVD anterior
            firstX,                // X acumulada
            firstY,                // Y acumulada
            interpolatedTVD,       // TVD interpolado
            tvdAbsolute            // TVD absoluto
          )
        } else if (md >= mdArray.last) {
          // Si el MD está fuera del rango por arriba, devolver el último valor
          val lastX = if (calculateXY) xArray.last else null
          val lastY = if (calculateXY) yArray.last else null
          val interpolatedTVD = tvdArray.last
          val tvdAbsolute = datumElevation - interpolatedTVD // Calcular TVD absoluto
          Row(
            incArray.last,         // Inclinación interpolada
            aziArray.last,         // Azimuth interpolado
            mdArray.last,          // MD anterior
            incArray.last,         // Inclinación anterior
            aziArray.last,         // Azimuth anterior
            tvdArray.last,         // TVD anterior
            lastX,                 // X acumulada
            lastY,                 // Y acumulada
            interpolatedTVD,       // TVD interpolado
            tvdAbsolute            // TVD absoluto
          )
        } else {
          // Encontrar los índices del MD anterior y el siguiente
          val idxOption = mdArray.indexWhere(_ > md) match {
            case -1 => None
            case idx => Some(idx)
          }

          idxOption match {
            case Some(idx) if idx > 0 =>
              val md1 = mdArray(idx - 1)          // MD anterior
              val md2 = mdArray(idx)              // MD siguiente
              val inc1Deg = incArray(idx - 1)     // Inclinación anterior en grados
              val inc2Deg = incArray(idx)         // Inclinación siguiente en grados
              val azi1Deg = aziArray(idx - 1)     // Azimuth anterior en grados
              val azi2Deg = aziArray(idx)         // Azimuth siguiente en grados
              val tvd1 = tvdArray(idx - 1)        // TVD en el punto anterior

              // Variables para X y Y si están disponibles
              val x1 = if (calculateXY) xArray(idx - 1) else null
              val y1 = if (calculateXY) yArray(idx - 1) else null

              // Interpolación lineal para inclinación en grados
              val ratio = (md - md1) / (md2 - md1)
              val interpolatedIncDeg = inc1Deg + ratio * (inc2Deg - inc1Deg)

              // Interpolación ajustada para azimuth en grados
              val interpolatedAziDeg = interpolateAngle(azi1Deg, azi2Deg, ratio)

              // Convertir ángulos a radianes para cálculos trigonométricos
              val inc1Rad = Math.toRadians(inc1Deg)
              val inc2Rad = Math.toRadians(interpolatedIncDeg)
              val azi1Rad = Math.toRadians(azi1Deg)
              val azi2Rad = Math.toRadians(interpolatedAziDeg)

              // Calcular el Dogleg Severity entre el punto anterior y el interpolado
              val cosDogLegSeverity = Math.cos(inc2Rad - inc1Rad) - Math.sin(inc1Rad) * Math.sin(inc2Rad) * (1 - Math.cos(azi2Rad - azi1Rad))
              // Clipping para evitar valores fuera de rango debido a errores numéricos
              val cosDogLegSeverityClipped = Math.max(-1.0, Math.min(1.0, cosDogLegSeverity))
              val dogLegSeverity = Math.acos(cosDogLegSeverityClipped)
              val dogLegFactor = if (dogLegSeverity != 0 && !dogLegSeverity.isNaN) (2 / dogLegSeverity) * Math.tan(dogLegSeverity / 2) else 1.0

              // Calcular ΔMD entre el punto anterior y el punto interpolado
              val deltaMD = md - md1

              // Calcular ΔX (Este) y ΔY (Norte) solo si podemos calcular XY
              val deltaX: java.lang.Double = if (calculateXY) {
                (deltaMD / 2 * (Math.sin(inc1Rad) * Math.sin(azi1Rad) + Math.sin(inc2Rad) * Math.sin(azi2Rad)) * dogLegFactor).toDouble
              } else null

              val deltaY: java.lang.Double = if (calculateXY) {
                (deltaMD / 2 * (Math.sin(inc1Rad) * Math.cos(azi1Rad) + Math.sin(inc2Rad) * Math.cos(azi2Rad)) * dogLegFactor).toDouble
              } else null

              // Acumular las diferencias a partir del punto anterior solo si podemos calcular XY
              val accumulatedX: java.lang.Double = if (calculateXY && x1 != null && deltaX != null) x1 + deltaX else null
              val accumulatedY: java.lang.Double = if (calculateXY && y1 != null && deltaY != null) y1 + deltaY else null

              // Calcular la diferencia en TVD (ΔTVD)
              val deltaTVD = deltaMD / 2 * (Math.cos(inc1Rad) + Math.cos(inc2Rad)) * dogLegFactor

              // Calcular el TVD interpolado sumando ΔTVD al TVD del punto anterior
              val interpolatedTVD = tvd1 + deltaTVD

              // Calcular el TVD absoluto
              val tvdAbsolute =  interpolatedTVD - datumElevation 

              // Devolver los valores interpolados y acumulados como un Row
              Row(
                interpolatedIncDeg,    // Inclinación interpolada
                interpolatedAziDeg,    // Azimuth interpolado
                md1,                   // MD anterior
                inc1Deg,               // Inclinación anterior
                azi1Deg,               // Azimuth anterior
                tvd1,                  // TVD anterior
                accumulatedX,          // X acumulada (puede ser null)
                accumulatedY,          // Y acumulada (puede ser null)
                interpolatedTVD,       // TVD interpolado
                tvdAbsolute            // TVD absoluto
              )
            case _ =>
              // Devolver nulos si no se encuentra un índice válido
              Row(null, null, null, null, null, null, null, null, null, null)
          }
        }
      }
    }
  },
  outputSchema // Especificamos el esquema de salida actualizado
)


// COMMAND ----------

import org.locationtech.proj4j.{CRSFactory, CoordinateReferenceSystem, ProjCoordinate, CoordinateTransformFactory}

// Función para convertir coordenadas UTM a Lat/Lon usando Proj4j
def utmToLatLonManual(utmX: Double, utmY: Double, systemType: String): (Double, Double) = {
  // Crear una fábrica para los sistemas de referencia
  val crsFactory = new CRSFactory()
  val transformFactory = new CoordinateTransformFactory()

  // Seleccionar el CRS según el sistema
  val sourceCRS: CoordinateReferenceSystem = systemType match {
    case "EF" => crsFactory.createFromName("EPSG:32040") // NAD27 / UTM zone 14N
    case "MBU" => crsFactory.createFromName("EPSG:3363") // NAD83 / UTM zone 18N
    case _ => return (Double.NaN, Double.NaN) // Sistema no válido
  }
  val targetCRS: CoordinateReferenceSystem = crsFactory.createFromName("EPSG:4326") // WGS84 Lat/Lon

  // Crear la transformación entre los sistemas de coordenadas
  val transform = transformFactory.createTransform(sourceCRS, targetCRS)

  // Crear las coordenadas de entrada y salida
  val srcCoord = new ProjCoordinate(utmX, utmY)
  val destCoord = new ProjCoordinate()

  // Transformar las coordenadas
  transform.transform(srcCoord, destCoord)

  // Retornar las coordenadas transformadas (latitud, longitud)
  (destCoord.y, destCoord.x)
}

// UDF para aplicar la conversión en el DataFrame
val utmToLatLonUDF = udf((utmX: Double, utmY: Double, state: String) => {
  val systemType = state match {
    case "TEXAS" => "EF"
    case "PENNSYLVANIA" | "NEW YORK" => "MBU"
    case _ => ""
  }
  val (lat, lon) = utmToLatLonManual(utmX, utmY, systemType)
  (lat, lon)
})

// Función principal para agregar las columnas de latitud y longitud al DataFrame
def addLatLonColumns(df: DataFrame): DataFrame = {
  df.withColumn("lat_lon", utmToLatLonUDF(df("val_stage_mid_perf_x_coord"), df("val_stage_mid_perf_y_coord"), df("des_state_or_province")))
    .withColumn("val_latitude", $"lat_lon._1")  // Extraer latitud
    .withColumn("val_longitude", $"lat_lon._2") // Extraer longitud
    .drop("lat_lon") // Eliminar la columna intermedia
}

// COMMAND ----------



def actualizarCampos(df: DataFrame): DataFrame = {
  df.withColumn("val_interval_top", col("val_interval_top") + col("val_datum_elevation"))
    .withColumn("val_interval_base", col("val_interval_base") + col("val_datum_elevation"))
    .withColumn("val_stage_mid_perf_depth", 
      col("val_interval_top") + ((col("val_interval_base") - col("val_interval_top")) / 2)
    )
}


// COMMAND ----------

def recalculateCompLength(df: DataFrame): DataFrame = {
  df.withColumn("val_completed_length", col("val_interval_base") - col("val_interval_top"))
}

// COMMAND ----------

def joinStimNullFill(dfL: DataFrame, dfR: DataFrame, key: String): DataFrame = {
  // Seleccionamos únicamente la clave y la columna de interés de dfR
  val dfRSubset = dfR.select(col(key), col("val_datum_elevation").alias("val_datum_elevation_R"))
  
  // Realizamos el join usando la clave
  val joinedDF = dfL.join(dfRSubset, Seq(key), "inner")
  
  // Con coalesce, usamos el valor de dfL o, si es null, el valor de dfR
  val resultDF = joinedDF.withColumn("val_datum_elevation",
    coalesce(col("val_datum_elevation"), col("val_datum_elevation_R"))
  )
  
  // Eliminamos la columna temporal y retornamos el DataFrame final
  resultDF.drop("val_datum_elevation_R").dropDuplicates()
}


// COMMAND ----------



var df: DataFrame  = null



try {

    val dfStimTreat = transformationDfStimTreat(getDataFromSource(sourcesFinal("aa001236_glb_dm_stim_treatment_t")))

    val dfStimJob = transformationStimJob(getDataFromSource(sourcesFinal("aa001236_glb_dm_stim_job_t")))

    val dfDevSurvey = transformationDevSurvey(getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_fac_d_def_surv_stat")))


    val dfWellboreEdm = transformationWellboreEDM(getDataFromSource(sourcesFinal("aa001236_glb_cd_wellbore_t")))

    val dfWellEdm = transformationWellEDM(getDataFromSource(sourcesFinal("aa001236_glb_cd_well_t")))

    val dfDimWell = transformationDimWell(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hole_unc")))

    val dfDimWellAll = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hole_unc")).select("cod_uwi", "val_datum_elevation")

    df = dfStimTreat

    df = joinStim(df, dfStimJob, "STIM_JOB_ID")

    df = joinStim(df, dfWellboreEdm, "WELLBORE_ID")

    df = joinStim(df, dfWellEdm, "WELL_ID")

    df = joinStim(df, dfDimWell, "cod_uwi")

    df = df.join(dfDevSurvey, Seq("WELLBORE_ID"), "left").withColumn("val_datum_elevation", col("val_datum_elevation").cast("double"))

    df = joinStimNullFill(df, dfDimWellAll, "cod_uwi")

    df = recalculateCompLength(df)

    df = actualizarCampos(df)

    df = df.withColumn("interpolatedData", minCurvatureInterpolateWithXY(col("val_stage_mid_perf_depth"), col("mdArray"), col("incArray"), 
                        col("aziArray"), col("tvdArray"), col("val_preferred_easting"), col("val_preferred_northing"), col("xArray"), col("yArray"), col("val_datum_elevation")))
                        .selectExpr("*", "interpolatedData.*")

        
    df = transformationFinalStimTreatTrn(df) 
    
    df = addLatLonColumns(df)
    
    df = df

    df = df.drop("cod_uwi")

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

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

import org.apache.spark.sql.functions._




def replaceNaNWithNull(df: DataFrame, col1: String, col2: String, col3: String, col4: String): DataFrame = {
  
  var dfTrn: DataFrame = df

  dfTrn = dfTrn.withColumn("new_col1", when(col(col1).isNaN, lit(null)).otherwise(col(col1)))
    .withColumn("new_col2", when(col(col2).isNaN, lit(null)).otherwise(col(col2)))
    .withColumn("new_col3", when(col(col3).isNaN, lit(null)).otherwise(col(col3)))
    .withColumn("new_col4", when(col(col4).isNaN, lit(null)).otherwise(col(col4)))
    .drop(col1, col2, col3, col4)
    .withColumnRenamed("new_col1", col1)
    .withColumnRenamed("new_col2", col2)
    .withColumnRenamed("new_col3", col3)
    .withColumnRenamed("new_col4", col4)
 
  return dfTrn 
}

val order = Seq("fec_start_date_rel","fec_end_date_rel","id_vers_rel","id_vers", "id_well_bore", "des_well_bore", "cod_uwbi", "cod_uwi", "cod_api12","des_wellbore_cds", "fec_start_date", 
"fec_end_date", "id_vers_well","id_vers_well_hole","id_well", "des_country", "ind_east_utm_td", "ind_north_utm_td", "ind_rkb_depth", "ind_sidetrack_depth", 
"ind_tvdss_depth", "ind_borehole_number", "des_common_name", "des_current_purpose", "fec_completed_date", 
"des_depth_ref_datum", "ind_depth_ref_elev", "des_depth_ref_elev_unit", "des_depth_ref_point", "des_drilling_operator", 
"des_drilling_permit", "id_field", "des_field", "ind_ground_elev", "des_ground_elev_unit", "des_innitial_purpose", 
"ind_kickoff_depth", "des_kickoff_depth_unit", "des_kind", "des_norm_bh_geo_coord_sys", 
"ind_normalized_bh_latitude", "ind_normalized_bh_longitude", "des_original_operator", "id_cds_parent", 
"des_pref_bh_geo_coord_sys", "ind_preferred_bh_latitude", "ind_preferred_bh_longitude", 
"des_pref_bh_prj_coord_sys", "des_pref_bh_prj_coord_unit", "ind_preffered_bh_easting", 
"ind_preffered_bh_northing", "fec_wellbore_spud_date", "ind_total_depth_final", "des_total_depth_final_unit", 
"ind_total_depth_planned", "des_total_depth_planned_unit", "des_trajectory_shape", "val_geo_offset_east_bh", "val_geo_offset_north_bh", 
"val_geo_latitude_bh","val_geo_longitude_bh","fec_date_td_reached","val_bh_md","val_bh_tvd","val_mid_lateral_x","val_mid_lateral_y","val_mid_lateral_md","val_mid_lateral_tvd",
"des_casing_p_phase", "val_casing_p_hole_size", "val_casing_p_diameter", "val_casing_p_shoe_depth","des_casing_1_phase", "val_casing_1_hole_size", "val_casing_1_diameter", 
"val_casing_1_shoe_depth","des_casing_2_phase", "val_casing_2_hole_size", "val_casing_2_diameter", "val_casing_2_shoe_depth","des_casing_3_phase", "val_casing_3_hole_size", 
"val_casing_3_diameter", "val_casing_3_shoe_depth", "bol_flag_tubing", "val_latitude", "val_longitude", "fec_create_date", "fec_update_date", "des_business_unit", "val_landing_md", "val_landing_tvd",
"val_landing_x", "val_landing_y", "val_landing_latitude", "val_landing_longitude")
             





val rowDummy = Row(java.sql.Timestamp.valueOf("1900-01-01 00:00:00"),java.sql.Timestamp.valueOf("1900-01-01 00:00:00"),"-99","-99" ,"-99", 
null, null, null, null, null, java.sql.Timestamp.valueOf("1900-01-01 00:00:00"), null, "-99","-99", null,  
null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
 null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null
, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null , null, null,
null, null, null, null, null, null) 


def addRowDummy(df: DataFrame): DataFrame = {
    
    var dfTrn: DataFrame = df

    val dfDummy = spark.createDataFrame(Seq(rowDummy).asJava, df.schema)

    dfTrn = dfTrn.union(dfDummy)

    return dfTrn

}

def addIdVers (df: DataFrame): DataFrame = {
    
   var dfTrn: DataFrame = df

   dfTrn = dfTrn.withColumn("id_vers_rel", when(col("id_well_bore") === "-99", concat(date_format(col("fec_start_date_rel"), "yyyyMMdd"), col("cod_uwbi")))
                .otherwise(concat(date_format(col("fec_start_date_rel"), "yyyyMMdd"), col("id_well_bore"), col("cod_uwbi"))))

   dfTrn = dfTrn.orderBy(desc("fec_start_date")).dropDuplicates("id_vers_rel")

   return dfTrn
    
}

def transformationDfWell (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("unique_wellbore_identifier", "cod_uwbi")
                .withColumnRenamed("unique_well_identifier", "cod_uwi")
                .withColumnRenamed("alternate_uwi", "cod_api12").withColumn("cod_api12", substring(regexp_replace(col("cod_api12"), "[^0-9]", ""),1, 12))
                .withColumnRenamed("wellbore_name", "des_wellbore_cds")
                .withColumnRenamed("borehole_number", "ind_borehole_number")
                .withColumnRenamed("common_name", "des_common_name")
                .withColumnRenamed("current_purpose", "des_current_purpose")
                .withColumnRenamed("date_completed", "fec_completed_date")
                .withColumnRenamed("depth_reference_datum", "des_depth_ref_datum")
                .withColumnRenamed("depth_reference_elevation", "ind_depth_ref_elev")
                .withColumnRenamed("depth_reference_elevation_unit", "des_depth_ref_elev_unit")
                .withColumnRenamed("depth_reference_point", "des_depth_ref_point")
                .withColumnRenamed("drilling_operator", "des_drilling_operator")
                .withColumnRenamed("drilling_permit_number", "des_drilling_permit")
                .withColumnRenamed("field_identifier", "id_field")
                .withColumnRenamed("field_name", "des_field")
                .withColumnRenamed("ground_elevation", "ind_ground_elev")
                .withColumnRenamed("ground_elevation_unit", "des_ground_elev_unit")
                .withColumnRenamed("initial_purpose", "des_innitial_purpose")
                .withColumnRenamed("kickoff_depth", "ind_kickoff_depth")
                .withColumnRenamed("kickoff_depth_unit", "des_kickoff_depth_unit")
                .withColumnRenamed("kind", "des_kind")
                .withColumnRenamed("normalized_bh_geographic_coordinate_sys", "des_norm_bh_geo_coord_sys")
                .withColumnRenamed("normalized_bh_latitude", "ind_normalized_bh_latitude").withColumn("ind_normalized_bh_latitude", col("ind_normalized_bh_latitude").cast("double"))
                .withColumnRenamed("normalized_bh_longitude", "ind_normalized_bh_longitude").withColumn("ind_normalized_bh_longitude", col("ind_normalized_bh_longitude").cast("double"))
                .withColumnRenamed("original_operator", "des_original_operator")
                .withColumnRenamed("parent_wellbore_identifier", "id_cds_parent")
                .withColumnRenamed("preferred_bh_geographic_coordinate_sys", "des_pref_bh_geo_coord_sys")
                .withColumnRenamed("preferred_bh_latitude", "ind_preferred_bh_latitude").withColumn("ind_preferred_bh_latitude", col("ind_preferred_bh_latitude").cast("double"))
                .withColumnRenamed("preferred_bh_longitude", "ind_preferred_bh_longitude").withColumn("ind_preferred_bh_longitude", col("ind_preferred_bh_longitude").cast("double"))
                .withColumnRenamed("preferred_bh_projected_coordinate_system", "des_pref_bh_prj_coord_sys")
                .withColumnRenamed("preferred_bh_projected_coordinate_unit", "des_pref_bh_prj_coord_unit")
                .withColumnRenamed("preferred_bh_easting", "ind_preffered_bh_easting").withColumn("ind_preffered_bh_easting", col("ind_preffered_bh_easting").cast("double"))
                .withColumnRenamed("preferred_bh_northing", "ind_preffered_bh_northing").withColumn("ind_preffered_bh_northing", col("ind_preffered_bh_northing").cast("double"))
                .withColumnRenamed("spud_date", "fec_wellbore_spud_date")
                .withColumnRenamed("total_depth_final", "ind_total_depth_final")
                .withColumnRenamed("total_depth_final_unit", "des_total_depth_final_unit")
                .withColumnRenamed("total_depth_planned", "ind_total_depth_planned")
                .withColumnRenamed("total_depth_planned_unit", "des_total_depth_planned_unit")
                .withColumnRenamed("trajectory_shape", "des_trajectory_shape")
                //.withColumnRenamed("alternate_uwi", "join_col_well_bore")
                .withColumnRenamed("update_date", "dateCDS")
                .withColumn("cod_uwbi", trim(regexp_replace(col("cod_uwbi"), "\\s+", "")))
                .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
                
   
    return dfTrn

}


def transformationDefSurHead (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df
    dfTrn = dfTrn.withColumnRenamed("BH_MD", "val_bh_md")
                .withColumnRenamed("BH_TVD", "val_bh_tvd")
                .select( "val_bh_md", "val_bh_tvd", "WELLBORE_ID", "UPDATE_DATE")

    return dfTrn
} 



//Join entre well_bore EC y wellbore CDS. 
def joinDfWellAndWellHole (dfL: DataFrame, dfR: DataFrame): DataFrame = {

    
    var resDf = dfL.join(dfR, Seq("cod_uwbi"), "right")

    resDf = resDf.withColumn("fec_start_date", to_timestamp(coalesce(col("fec_start_date"), lit("1970-01-01"))))
                 .withColumn("fix_id_vers", when(col("id_well_bore").isNull, 
                 concat(date_format(col("fec_start_date"), "yyyyMMdd"), col("cod_uwbi")))
                 .otherwise(concat(date_format(col("fec_start_date"), "yyyyMMdd"), col("id_well_bore"), col("cod_uwbi"))))
                 .drop("id_vers")
                 .withColumnRenamed("fix_id_vers", "id_vers")
                 .withColumn("id_well_bore", coalesce(col("id_well_bore"), lit("-99")))
                 .withColumn("fec_end_date_aux", when(col("fec_end_date").isNull, to_timestamp(lit("9999-12-31 00:00:00")))
                                                                            .otherwise(col("fec_end_date")))

    return resDf

}


def joinWellboreAndWell (dfL: DataFrame, dfR: DataFrame): DataFrame = {
    

    // var resDf = dfL.join(dfR, dfL("master_sys_code") === dfR("id_cds") && dfR("dateCDS") <= dfL("fec_start_date"), "left")
    var resDf = dfL.join(dfR,
                        dfL("id_well") ===  dfR("id_well_2") &&
                        dfL("fec_end_date_aux") >=  dfR("fec_start_date_well") &&
                        dfL("fec_start_date") <  dfR("fec_end_date_well_aux"), 
                        "left"
                        )
                        .withColumn("fec_start_date_rel", when(col("fec_start_date_well").isNull, col("fec_start_date"))
                        .otherwise(greatest(col("fec_start_date"), col("fec_start_date_well"))))
                        .withColumn("fec_end_date_rel", when(col("fec_end_date_well_aux").isNull, col("fec_end_date_aux"))
                        .otherwise(least(col("fec_end_date_aux"), col("fec_end_date_well_aux"))))
                        .withColumn("id_vers_well", coalesce(dfR("id_vers_well"), lit("-99")))
                        .withColumn("fec_end_date_rel_fix", when(col("fec_end_date_rel") === to_timestamp(lit("9999-12-31 00:00:00")), col("fec_end_date_rel"))
                        .otherwise(to_timestamp(date_sub(to_date(col("fec_end_date_rel")), 1))))
                        .drop("fec_end_date_rel")
                        .withColumnRenamed("fec_end_date_rel_fix", "fec_end_date_rel")
                        .filter(col("fec_start_date_rel") <= col("fec_end_date_rel"))
                        .withColumn("fec_end_date_rel_fix", when(col("fec_end_date_rel") === to_timestamp(lit("9999-12-31 00:00:00")), lit(null))
                        .otherwise(col("fec_end_date_rel")))
                        .drop("fec_end_date_rel")
                        .withColumnRenamed("fec_end_date_rel_fix", "fec_end_date_rel")

    return resDf
}


def joinCdsEdm (dfL: DataFrame, dfR: DataFrame, orderCol: String, keyCol: String): DataFrame = {

    val hWin = Window.partitionBy(col("id_vers"), col("fec_start_date")).orderBy(col(orderCol).desc)

    // var resDf = dfL.join(dfR, dfL("master_sys_code") === dfR("id_cds") && dfR("dateCDS") <= dfL("fec_start_date"), "left")
    var resDf = dfL.join(dfR, Seq(keyCol), "left")

    return resDf.withColumn("new", row_number().over(hWin) ).where(col("new") === 1).drop(orderCol)

}
 
 
 def transformationDevSurvey (df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn.orderBy("WELLBORE_ID", "val_station_md").groupBy("WELLBORE_ID")
                                                        .agg(
                                                        collect_list("val_station_md").as("mdArray"),
                                                        collect_list("val_station_dev").as("incArray"),
                                                        collect_list("val_station_az").as("aziiArray"),
                                                        collect_list("val_station_az_grid").as("aziArray"),
                                                        collect_list("val_station_tvd").as("tvdArray"),
                                                        collect_list("x_interpolado").as("xArray"),
                                                        collect_list("y_interpolado").as("yArray"),
                                                        first("val_preferred_easting").as("val_preferred_easting"),
                                                        first("val_preferred_northing").as("val_preferred_northing"),
                                                        first("cod_uwbi").as("cod_uwbi"),
                                                        first("val_geo_offset_east_bh").as("val_geo_offset_east_bh"),
                                                        first("val_geo_offset_north_bh").as("val_geo_offset_north_bh"),
                                                        first("val_geo_latitude_bh").as("val_geo_latitude_bh"),
                                                        first("val_geo_longitude_bh").as("val_geo_longitude_bh"),
                                                        first("fec_date_td_reached").as("fec_date_td_reached"),
                                                        first("WELLBORE_ID").as("WELLBORE_ID"),
                                                        first("UPDATE_DATE").as("UPDATE_DATE"))
    return dfTrn
                                }

// COMMAND ----------

// MAGIC %md
// MAGIC # Función de interpolación 

// COMMAND ----------

// DBTITLE 1,Cell 9
import java.lang.Math

// spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")  // setup in config

// Definir el esquema de salida
val outputSchema = StructType(Seq(
  StructField("val_mid_lateral_md", DoubleType, nullable = true),
  StructField("val_mid_lateral_tvd", DoubleType, nullable = true),
  StructField("val_mid_lateral_x", DoubleType, nullable = true),
  StructField("val_mid_lateral_y", DoubleType, nullable = true)
))

// Función para interpolar ángulos considerando su naturaleza circular
def interpolateAngle(angle1Deg: java.lang.Double, angle2Deg: java.lang.Double, ratio: java.lang.Double): java.lang.Double = {
  if (angle1Deg == null || angle2Deg == null || ratio == null) {
    null
  } else {
    val deltaAngle = ((angle2Deg - angle1Deg + 540) % 360) - 180 // Diferencia angular mínima
    val interpolatedAngle = (angle1Deg + ratio * deltaAngle + 360) % 360
    if (java.lang.Double.isNaN(interpolatedAngle) || java.lang.Double.isInfinite(interpolatedAngle)) null else interpolatedAngle
  }
}

// Función principal para calcular val_mid_lateral_md, val_mid_lateral_tvd, val_mid_lateral_x y val_mid_lateral_y
val calculateMidLateral = udf(
  (
    mdArray: Seq[java.lang.Double],
    incArray: Seq[java.lang.Double],
    aziArray: Seq[java.lang.Double],
    tvdArray: Seq[java.lang.Double],
    xArray: Seq[java.lang.Double],
    yArray: Seq[java.lang.Double],
    initialX: java.lang.Double,
    initialY: java.lang.Double,
    val_bh_md: java.lang.Double
  ) => {
    // Verificar que los arrays esenciales no sean nulos y tengan el mismo tamaño
    if (mdArray == null || incArray == null || aziArray == null || tvdArray == null || val_bh_md == null) {
      Row(null, null, null, null)
    } else if (mdArray.size != incArray.size || mdArray.size != aziArray.size || mdArray.size != tvdArray.size) {
      Row(null, null, null, null)
    } else {
      val n = mdArray.size

      // Encontrar el índice donde la inclinación supera los 80 grados
      val idxOption = incArray.indexWhere(inc => inc != null && inc >= 80.0) match {
        case -1 => None
        case idx => Some(idx)
      }

      idxOption match {
        case Some(idx) =>
          // Obtener md_start
          val md_start = mdArray(idx)
          if (md_start == null) {
            Row(null, null, null, null)
          } else {
            // Calcular val_mid_lateral_md
            val val_mid_lateral_md = md_start + (val_bh_md - md_start) / 2.0

            // Verificar que val_mid_lateral_md no sea NaN o Infinity
            val validMidLateralMD: java.lang.Double = if (java.lang.Double.isNaN(val_mid_lateral_md) || java.lang.Double.isInfinite(val_mid_lateral_md)) null else val_mid_lateral_md

            if (validMidLateralMD == null) {
              Row(null, null, null, null)
            } else {
              // Interpolar en val_mid_lateral_md utilizando los dos puntos más cercanos
              val indexBelowOption = mdArray.lastIndexWhere(md => Option(md).exists(_.doubleValue() <= validMidLateralMD)) match {
                case -1 => None
                case idx => Some(idx)
              }

              val indexAboveOption = mdArray.indexWhere(md => Option(md).exists(_.doubleValue() >= validMidLateralMD)) match {
                case -1 => None
                case idx => Some(idx)
              }

              if (indexBelowOption.isEmpty || indexAboveOption.isEmpty) {
                // No se puede interpolar fuera del rango
                Row(null, null, null, null)
              } else {
                val idx1 = indexBelowOption.get
                val idx2 = indexAboveOption.get

                // Manejar el caso cuando idx1 == idx2
                if (idx1 == idx2) {
                  // val_mid_lateral_md coincide con un MD conocido
                  val val_mid_lateral_tvd = tvdArray(idx1)
                  
                  // Verificar que val_mid_lateral_tvd no sea NaN o Infinity
                  val validMidLateralTVD = if (java.lang.Double.isNaN(val_mid_lateral_tvd) || java.lang.Double.isInfinite(val_mid_lateral_tvd)) null else val_mid_lateral_tvd
                  
                  val canCalculateXY = initialX != null && initialY != null && 
                    xArray != null && yArray != null && xArray.size == n && yArray.size == n

                  val val_mid_lateral_x = if (canCalculateXY) {
                    Option(xArray(idx1)) match {
                      case Some(x) => 
                        val xDouble = x.toDouble
                        if (java.lang.Double.isNaN(xDouble) || java.lang.Double.isInfinite(xDouble)) null else xDouble
                      case None => null
                    }
                  } else null

                  val val_mid_lateral_y = if (canCalculateXY) {
                    Option(yArray(idx1)) match {
                      case Some(y) =>
                        val yDouble = y.toDouble
                        if (java.lang.Double.isNaN(yDouble) || java.lang.Double.isInfinite(yDouble)) null else yDouble
                      case None => null
                    }
                  } else null

                  Row(validMidLateralMD, validMidLateralTVD, val_mid_lateral_x, val_mid_lateral_y)
                } else {
                  val md1 = Option(mdArray(idx1)).map(_.doubleValue()).getOrElse(null.asInstanceOf[Double])
                  val md2 = Option(mdArray(idx2)).map(_.doubleValue()).getOrElse(null.asInstanceOf[Double])
                  val inc1Deg = incArray(idx1)
                  val inc2Deg = incArray(idx2)
                  val azi1Deg = aziArray(idx1)
                  val azi2Deg = aziArray(idx2)
                  val tvd1 = tvdArray(idx1)

                  if (md1 == null || md2 == null || inc1Deg == null || inc2Deg == null || azi1Deg == null || azi2Deg == null || tvd1 == null) {
                    Row(null, null, null, null)
                  } else {
                    // Calcular el ratio para la interpolación
                    val ratio = (validMidLateralMD - md1) / (md2 - md1)

                    // Interpolar inclinación y azimuth
                    val incInterpolatedDeg = inc1Deg + ratio * (inc2Deg - inc1Deg)
                    val aziInterpolatedDeg = interpolateAngle(azi1Deg, azi2Deg, ratio)

                    // Convertir ángulos a radianes
                    val inc1Rad = Math.toRadians(inc1Deg)
                    val inc2Rad = Math.toRadians(incInterpolatedDeg)
                    val azi1Rad = Math.toRadians(azi1Deg)
                    val azi2Rad = Math.toRadians(aziInterpolatedDeg)

                    // Calcular Dogleg Severity y Dogleg Factor
                    val deltaAzi = azi2Rad - azi1Rad
                    val deltaInc = inc2Rad - inc1Rad
                    val cosDogleg = Math.cos(deltaInc) - Math.sin(inc1Rad) * Math.sin(inc2Rad) * (1 - Math.cos(deltaAzi))
                    val cosDoglegClipped = Math.max(-1.0, Math.min(1.0, cosDogleg))
                    val dogleg = Math.acos(cosDoglegClipped)
                    val doglegFactor = if (dogleg == 0 || dogleg.isNaN) 1.0 else (2.0 / dogleg) * Math.tan(dogleg / 2.0)

                    val deltaMD = validMidLateralMD - md1

                    // Calcular delta TVD
                    val deltaTVD = (deltaMD / 2.0) * (Math.cos(inc1Rad) + Math.cos(inc2Rad)) * doglegFactor
                    val val_mid_lateral_tvd = tvd1 + deltaTVD

                    // Verificar que val_mid_lateral_tvd no sea NaN o Infinity
                    val validMidLateralTVD = if (java.lang.Double.isNaN(val_mid_lateral_tvd) || java.lang.Double.isInfinite(val_mid_lateral_tvd)) null else val_mid_lateral_tvd

                    // Verificar si podemos calcular X e Y
                    val canCalculateXY = initialX != null && initialY != null && initialX != 0.0f && initialY != 0.0f &&
                      xArray != null && yArray != null && xArray.size == n && yArray.size == n

                    val val_mid_lateral_x = if (canCalculateXY) {
                      Option(xArray(idx1)) match {
                        case Some(x1) =>
                          val x1Double = x1.toDouble
                          val deltaEasting = (deltaMD / 2.0) * (Math.sin(inc1Rad) * Math.sin(azi1Rad) + Math.sin(inc2Rad) * Math.sin(azi2Rad)) * doglegFactor
                          val resultX = x1Double + deltaEasting
                          // Verificación de NaN e Infinity para val_mid_lateral_x
                          if (java.lang.Double.isNaN(resultX) || java.lang.Double.isInfinite(resultX)) null else resultX
                        case None => null
                      }
                    } else null

                    val val_mid_lateral_y = if (canCalculateXY) {
                      Option(yArray(idx1)) match {
                        case Some(y1) =>
                          val y1Double = y1.toDouble
                          val deltaNorthing = (deltaMD / 2.0) * (Math.sin(inc1Rad) * Math.cos(azi1Rad) + Math.sin(inc2Rad) * Math.cos(azi2Rad)) * doglegFactor
                          val resultY = y1Double + deltaNorthing
                          // Verificación de NaN e Infinity para val_mid_lateral_y
                          if (java.lang.Double.isNaN(resultY) || java.lang.Double.isInfinite(resultY)) null else resultY
                        case None => null
                      }
                    } else null

                    Row(validMidLateralMD, validMidLateralTVD, val_mid_lateral_x, val_mid_lateral_y)
                  }
                }
              }
            }
          }

        case None =>
          // No se encontró inclinación mayor o igual a 80 grados
          Row(null, null, null, null)
      }
    }
  },
  outputSchema
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
  df.withColumn("lat_lon", utmToLatLonUDF(df("val_mid_lateral_x"), df("val_mid_lateral_y"), df("des_state_or_province")))
    .withColumn("val_latitude", $"lat_lon._1")  // Extraer latitud
    .withColumn("val_longitude", $"lat_lon._2") // Extraer longitud
    .drop("lat_lon") // Eliminar la columna intermedia
}

// Función principal para agregar las columnas de latitud y longitud al DataFrame
def addLatLonColumnsLanding(df: DataFrame): DataFrame = {
  df.withColumn("lat_lon", utmToLatLonUDF(df("val_landing_x"), df("val_landing_y"), df("des_state_or_province")))
    .withColumn("val_landing_latitude", $"lat_lon._1")  // Extraer latitud
    .withColumn("val_landing_longitude", $"lat_lon._2") // Extraer longitud
    .drop("lat_lon") // Eliminar la columna intermedia
}


// COMMAND ----------

def filterBusinessUnits(df: DataFrame): DataFrame = {
  df.filter($"des_business_unit".isin("EagleFord", "Marcellus"))
}

// COMMAND ----------

/*import org.locationtech.proj4j.{CRSFactory, CoordinateReferenceSystem, ProjCoordinate, CoordinateTransformFactory}

val crsFactory = new CRSFactory();
val crs_origen = crsFactory.createFromName("EPSG:32040")
val crs_destino = crsFactory.createFromName("EPSG:4326")

val utm_x = 2372709.2
val utm_y = 355734.24

val coor = new ProjCoordinate(utm_x, utm_y)

val ctFactory = new CoordinateTransformFactory();

val trans = ctFactory.createTransform(crs_origen, crs_destino)
var pout = new ProjCoordinate();
trans.transform(coor,pout);
println(s"Latitud: ${pout.y}, Longitud: ${pout.x}")*/

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.math.Pi

def computeLandingMdAndDls(
    df: DataFrame, 
    dfWellboreDevSurvey: DataFrame
): DataFrame = {
  // Paso 0: Renombrar columnas duplicadas para evitar ambigüedades en dfWellboreDevSurvey.
  val cols = dfWellboreDevSurvey.columns
  var seen = Set[String]()
  val newCols = cols.map { colName =>
    if (colName == "WELLBORE_ID") {
      if (seen.contains(colName)) s"${colName}_dup" else { seen += colName; colName }
    } else colName
  }
  val dfWellboreDevSurveyFixed = dfWellboreDevSurvey.toDF(newCols: _*)

  // Paso 1: Explosión de los arrays en dfWellboreDevSurveyFixed.
  // Se combinan los arrays (mdArray, incArray, aziArray, tvdArray, xArray, yArray)
  // y se explota para obtener una fila por medición.
  val surveyExploded = dfWellboreDevSurveyFixed
    .withColumn("survey", arrays_zip(
      col("mdArray"),
      col("incArray"),
      col("aziArray"),    // Usamos este array para el azimuth
      col("tvdArray"),
      col("xArray"),
      col("yArray")
    ))
    .withColumn("survey", explode(col("survey")))
    .select(
      col("cod_uwbi"),                        // Clave para particionar y unir
      col("WELLBORE_ID").alias("surveyWellboreId"),
      col("survey.mdArray").alias("md"),
      col("survey.incArray").alias("inc"),
      col("survey.aziArray").alias("azi"),
      col("survey.tvdArray").alias("tvd"),
      col("survey.xArray").alias("x"),
      col("survey.yArray").alias("y"),
      col("val_preferred_easting"),
      col("val_preferred_northing")
    )
  
  // Paso 2: Definir una ventana basada en cod_uwbi ordenada por md.
  val windowSpec = Window.partitionBy("cod_uwbi").orderBy("md")
  
  // Paso 3: Calcular los valores de la fila anterior (lag)
  val surveyWithLag = surveyExploded
    .withColumn("prev_md", lag("md", 1).over(windowSpec))
    .withColumn("prev_inc", lag("inc", 1).over(windowSpec))
    .withColumn("prev_azi", lag("azi", 1).over(windowSpec))
  
  // Paso 4: Calcular la Dogleg Severity (DLS) para cada segmento.
  // Las columnas que intervienen directamente son:
  // - md y prev_md
  // - inc y prev_inc
  // - azi y prev_azi
  // Si alguna de ellas es null, el resultado (y por ende val_landing_md y dls) será null.
  val surveyWithDogleg = surveyWithLag
    .withColumn("deltaMd", col("md") - col("prev_md"))
    .withColumn("cosDogleg", 
      cos( radians(col("inc") - col("prev_inc")) ) -
      sin( radians(col("prev_inc")) ) * sin( radians(col("inc")) ) *
      (lit(1) - cos( radians(col("azi") - col("prev_azi")) ))
    )
    .withColumn("doglegAngle", acos(col("cosDogleg")))
    .withColumn("conversionFactor", lit((180.0 / Pi) * 100))
    .withColumn("val_dogleg", (col("doglegAngle") / col("deltaMd")) * col("conversionFactor"))
  
  // Paso 5: Marcar el punto candidato: donde inc > 75 y DLS < 4.
  // El landing point se define como el punto inmediatamente anterior al candidato.
  val surveyWithCandidate = surveyWithDogleg.withColumn("candidate", 
    when(col("inc") > 75 && col("val_dogleg") < 4, true).otherwise(false)
  )
  
  // Paso 6: Trasladar la marca del candidato a la fila anterior mediante lead.
  // La fila que tenga isLanding = true es el landing point.
  val surveyWithLandingFlag = surveyWithCandidate.withColumn("isLanding", lead("candidate", 1).over(windowSpec))
  
  // Paso 7: Extraer el MD y el DLS del landing point para cada cod_uwbi.
  // Es decir, se extrae la información de la fila que es landing point.
  val landingPoints = surveyWithLandingFlag
    .filter(col("isLanding") === true)
    .select(
      col("cod_uwbi"),
      col("md").alias("val_landing_md"),
      col("val_dogleg").alias("dls")
    )
  
  // Si hay más de un landing point por pozo, se selecciona el de menor md.
  val landingPointsUnique = landingPoints
    .withColumn("rn", row_number().over(Window.partitionBy("cod_uwbi").orderBy("val_landing_md")))
    .filter(col("rn") === 1)
    .drop("rn")
  
  // Paso 8: Unir la información de val_landing_md y dls con el DataFrame principal df usando cod_uwbi.
  val dfWithLanding = df.alias("a")
    .join(landingPointsUnique.alias("b"), col("a.cod_uwbi") === col("b.cod_uwbi"), "left")
    .selectExpr("a.*", "b.val_landing_md", "b.dls")
  
  dfWithLanding
}


// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Para cada valor distinto de `idCol`, conserva únicamente la fila cuya `dateCol` sea máxima.
 *
 * @param df      DataFrame de entrada.
 * @param idCol   Nombre de la columna de ID (en tu caso: "id_vers").
 * @param dateCol Nombre de la columna de fecha (en tu caso: "fec_start_date_rel").
 *                Si viene como String, internamente la convierte a Timestamp; 
 *                si ya es Date/Timestamp, la deja igual.
 * @return DataFrame filtrado sin duplicados de idCol, quedándote sólo con la fila de fecha más reciente.
 */
def keepMostRecentById(df: DataFrame, idCol: String, dateCol: String): DataFrame = {
  // 1) Si dateCol viene como StringType, conviértela a TimestampType (ajusta el formato si tus fechas no son "yyyy-MM-dd HH:mm:ss").
  val df2 = df.schema(dateCol).dataType.typeName match {
    case "string" =>
      df.withColumn(dateCol, to_timestamp(col(dateCol), "yyyy-MM-dd HH:mm:ss"))
    case _ =>
      df
  }

  // 2) Definimos la ventana: particionada por idCol, ordenada descendentemente por dateCol
  val windowSpec = Window
    .partitionBy(col(idCol))
    .orderBy(col(dateCol).desc)

  // 3) Asignamos row_number() dentro de cada partición; mantenemos sólo rn == 1
  df2
    .withColumn("rn", row_number().over(windowSpec))
    .filter(col("rn") === 1)
    .drop("rn")
}


// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

def fillBusinessUnitFromCDS(
  df: DataFrame,
  dfCDSWell: DataFrame,
  codUwiCol: String = "cod_uwi"
): DataFrame = {

  // Normaliza df
  val dfPrepared = df
    .withColumn("cod_uwi_norm", upper(trim(col(codUwiCol))))
    .withColumn(
      "des_business_unit",
      when(trim(col("des_business_unit")) === "" || col("des_business_unit").isNull, lit(null:String))
        .otherwise(col("des_business_unit"))
    )

  // Normaliza dfCDSWell y mapea estado -> BU
  val dfMap = dfCDSWell
    .withColumn("cod_uwi_norm", upper(trim(col("cod_uwi"))))
    .withColumn("state_norm", lower(trim(col("des_business_unit")))) // aquí "des_business_unit" es el estado
    .withColumn(
      "bu_from_state",
      when(col("state_norm") === "texas", lit("EagleFord"))
        .when(col("state_norm") === "pennsylvania", lit("Marcellus"))
        .when(col("state_norm") === "new york", lit("Marcellus"))
    )
    .filter(col("bu_from_state").isNotNull)
    .select("cod_uwi_norm", "bu_from_state")
    .dropDuplicates("cod_uwi_norm")

  val dfMapOptimized = broadcast(dfMap)

  // Join y completar solo si df está vacío
  val result = dfPrepared
    .join(dfMapOptimized, Seq("cod_uwi_norm"), "left")
    .withColumn(
      "des_business_unit",
      coalesce(col("des_business_unit"), col("bu_from_state"))
    )
    .drop("cod_uwi_norm", "bu_from_state")

  result
}


// COMMAND ----------

var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {
        
        val dfWellboreCDS: DataFrame = transformationDfWell(getDataFromSource(sourcesFinal("aa001163_glb_cds_wellbore")))


        val dfWellBore: DataFrame = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_dim_d_well_bore")).filter(col("id_vers") =!= "-99")
                                                                            .withColumn("ind_east_utm_td", col("ind_east_utm_td").cast("double"))
                                                                            .withColumn("ind_north_utm_td", col("ind_north_utm_td").cast("double"))
                                                                            .withColumn("ind_rkb_depth", col("ind_rkb_depth").cast("double"))
                                                                            .withColumn("ind_sidetrack_depth", col("ind_sidetrack_depth").cast("double"))
                                                                            .withColumn("ind_tvdss_depth", col("ind_tvdss_depth").cast("double"))
                                                                            .withColumnRenamed("master_sys_code", "cod_uwbi")
                                                                            
                                                                    

        val dfWellboreDevSurvey: DataFrame = transformationDevSurvey(getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_fac_d_def_surv_stat"))).withColumn("cod_uwbi", trim(regexp_replace(col("cod_uwbi"), "\\s+", "")))

        val dfDefSurvHead: DataFrame = transformationDefSurHead(getDataFromSource(sourcesFinal("aa001236_glb_cd_definitive_survey_header_t")))

        val dfWell: DataFrame = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well")).select("id_vers", "id_well", "fec_start_date", "fec_end_date")
                                                                        .withColumnRenamed("id_vers", "id_vers_well")
                                                                        .withColumnRenamed("id_well", "id_well_2")
                                                                        .withColumnRenamed("fec_start_date", "fec_start_date_well")
                                                                        .withColumnRenamed("fec_end_date", "fec_end_date_well")
                                                                        .withColumn("fec_end_date_well_aux", when(col("fec_end_date_well").isNull, to_timestamp(lit("9999-12-31 00:00:00")))
                                                                        .otherwise(col("fec_end_date_well")))
                                                                        

        val dfWellDesign: DataFrame = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_dim_d_well_design"))

        val dfWellHole: DataFrame = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hole_unc")).withColumnRenamed("id_vers", "id_vers_well_hole")
                                                                            .orderBy(desc("fec_start_date"))
                                                                            .dropDuplicates("cod_uwi")
                                                                            .select("id_vers_well_hole", "cod_uwi", "des_state_or_province", "des_business_unit")
                                                                            .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))

        val dfCDSWell = getDataFromSource(sourcesFinal("aa001163_glb_cds_well")).select("unique_well_identifier", "state_or_province")
                                                                .withColumnRenamed("unique_well_identifier", "cod_uwi")
                                                                .withColumnRenamed("state_or_province", "des_business_unit")

        // dfWell.printSchema()
        // dfWellHole(master_sys_code) - left - dfWell(unique_well_identifier)

        df = joinDfWellAndWellHole(dfWellBore, dfWellboreCDS)

        df = joinCdsEdm(df, dfWellboreDevSurvey, "UPDATE_DATE", "cod_uwbi")

        df = joinCdsEdm(df, dfDefSurvHead, "UPDATE_DATE", "WELLBORE_ID")

        df = df.withColumn("val_bh_md", col("val_bh_md").cast(DoubleType))

        

        df = df.withColumn("midLateralData", calculateMidLateral(
                                                                col("mdArray"),
                                                                col("incArray"),
                                                                col("aziArray"),
                                                                col("tvdArray"),
                                                                col("xArray"),
                                                                col("yArray"),
                                                                col("val_preferred_easting"),
                                                                col("val_preferred_northing"),
                                                                col("val_bh_md")
                                                                ))

        df = df.withColumn("val_mid_lateral_md", col("midLateralData.val_mid_lateral_md"))
                .withColumn("val_mid_lateral_tvd", col("midLateralData.val_mid_lateral_tvd"))
                .withColumn("val_mid_lateral_x", col("midLateralData.val_mid_lateral_x"))
                .withColumn("val_mid_lateral_y", col("midLateralData.val_mid_lateral_y"))
                .drop("midLateralData")

        df = replaceNaNWithNull(df, "val_mid_lateral_x", "val_mid_lateral_y", "val_mid_lateral_tvd", "val_mid_lateral_md")

        df = df.join(dfWellDesign, Seq("WELLBORE_ID"), "left")

        df = joinWellboreAndWell(df, dfWell)

        df = df.join(dfWellHole, Seq("cod_uwi"), "left").withColumn("id_vers_well_hole", coalesce(col("id_vers_well_hole"), lit("-99")))
        df = fillBusinessUnitFromCDS(df, dfCDSWell, "cod_uwi")

        df = addLatLonColumns(df)   

        df = computeLandingMdAndDls(df, dfWellboreDevSurvey)    

        df = df.withColumn("midLateralData", calculateMidLateral(
                                                                col("mdArray"),
                                                                col("incArray"),
                                                                col("aziArray"),
                                                                col("tvdArray"),
                                                                col("xArray"),
                                                                col("yArray"),
                                                                col("val_preferred_easting"),
                                                                col("val_preferred_northing"),
                                                                col("val_landing_md")
                                                                ))

        df = df.withColumn("val_landing_md", col("midLateralData.val_mid_lateral_md"))
                .withColumn("val_landing_tvd", col("midLateralData.val_mid_lateral_tvd"))
                .withColumn("val_landing_x", col("midLateralData.val_mid_lateral_x"))
                .withColumn("val_landing_y", col("midLateralData.val_mid_lateral_y"))
                .drop("midLateralData")

        df = addLatLonColumnsLanding(df)

        df = addIdVers(df)

        df = filterBusinessUnits(df)

        df = df.select(order.map(col): _*)

        df = addRowDummy(df)

        val countExpr = df.columns.map(colName => when(col(colName).isNotNull, 1).otherwise(0)).reduce(_ + _)

        // Convertir totalColumns a columna literal para la división
        val totalColumns = df.columns.length

        // Añadir la nueva columna que divide la suma por el total de columnas y redondear a 3 decimales
        df = df.withColumn("val_proportion_non_nulls", bround(countExpr / totalColumns, 3))


        df = keepMostRecentById(df, "id_vers", "fec_start_date_rel")
        
        val totalFilas = df.count()

        // 2. Conteo de valores distintos en “id_vers”
        val totalDistinct = df.select("id_vers").distinct().count()

        println(s"Total de filas = $totalFilas")
        println(s"Total de id_vers distintos = $totalDistinct")

        val dfFormatted = applyDDLFromTableToDF(df,getObject((target_object).toString))

        writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

    println("Se han insertado "+df.count+ " registros")

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

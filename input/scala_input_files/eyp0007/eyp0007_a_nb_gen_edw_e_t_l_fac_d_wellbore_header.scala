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


val orderFields: Seq[String] = Seq(
  "cod_uwbi", "cod_uwi", "des_country_name", "des_wellbore_name", "num_wellbore", "des_drilling_permit_number", "cod_parent_uwi", 
  "cod_field", "des_field_name", "des_field_type", "des_lease_contract_id", "des_original_operator", 
  "des_drilling_operator", "des_target_formation", "des_play_type", "val_total_depth_planned", "des_total_depth_planned_unit", "val_total_depth_final",
  "des_total_depth_final_unit", "val_kickoff_depth", "des_kickoff_depth_unit", "num_rig_days", "des_kind", "des_initial_purpose", 
  "des_current_purpose", "des_current_status", "fec_current_status_date", "fec_date_completed", "des_wellbore_fluid", "des_trajectory_shape", 
  "val_water_depth", "des_water_depth_unit", "des_water_depth_datum", "cod_alt_uwi_wellbore", "val_ground_elevation", "des_ground_elevation_unit", 
  "des_description", "fec_create", "cod_create_user", "fec_update", "cod_update_user", "val_wellbore_bh_easting", "val_wellbore_bh_northing", "des_bh_proj_coord_unit",
  "des_bh_proj_coord_sys", "des_date_type", "fec_date_val", "des_basin_identifier", "des_preferred_geographic_coordinate_system",
  "des_preferred_projected_coordinate_system","val_preferred_latitude", "val_preferred_longitude", "des_well_name", "val_preferred_easting", 
  "val_preferred_northing", "des_gov_identifier", "fec_proposed_drill_start", "fec_proposed_drill_finish", "fec_drill_start", "fec_drill_finished", 
  "fec_rig_release", "fec_completion", "fec_spud", "fec_drilling_permit", "des_data_source", "des_depth_reference_kind", "des_elevation_datum",
    "val_elevation", "des_depth_elevation_unit", "des_elevation_type", "des_common_name", "des_wellbore_bh_geo_coord_sys", 
    "val_wellbore_bh_latitude", "val_wellbore_bh_longitude", "val_proposed_total_depth_tvd", "des_proposed_total_depth_tvd_unit", "val_final_total_depth_tvd",
    "des_final_total_depth_tvd_unit"
)


def transformDfWellbore(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("unique_wellbore_identifier", "cod_uwbi").withColumn("cod_uwbi", col("cod_uwbi").cast("string"))
    .withColumnRenamed("unique_well_identifier", "cod_uwi").withColumn("cod_uwi", col("cod_uwi").cast("string"))
    .withColumnRenamed("wellbore_number", "num_wellbore").withColumn("num_wellbore", col("num_wellbore").cast("double"))
    .withColumnRenamed("alternate_uwi", "cod_alt_uwi_wellbore").withColumn("cod_alt_uwi_wellbore", col("cod_alt_uwi_wellbore").cast("string"))
    .withColumnRenamed("wellbore_name", "des_wellbore_name").withColumn("des_wellbore_name", col("des_wellbore_name").cast("string"))
    .withColumnRenamed("country_name", "des_country_name").withColumn("des_country_name", col("des_country_name").cast("string"))
    .withColumnRenamed("parent_wellbore_identifier", "cod_parent_uwi").withColumn("cod_parent_uwi", col("cod_parent_uwi").cast("string"))
    .withColumnRenamed("trajectory_shape", "des_trajectory_shape").withColumn("des_trajectory_shape", col("des_trajectory_shape").cast("string"))
    .withColumnRenamed("kickoff_depth", "val_kickoff_depth").withColumn("val_kickoff_depth", col("val_kickoff_depth").cast("double"))
    .withColumnRenamed("kickoff_depth_unit", "des_kickoff_depth_unit").withColumn("des_kickoff_depth_unit", col("des_kickoff_depth_unit").cast("string"))
    .withColumnRenamed("original_operator", "des_original_operator").withColumn("des_original_operator", col("des_original_operator").cast("string"))
    .withColumnRenamed("drilling_operator", "des_drilling_operator").withColumn("des_drilling_operator", col("des_drilling_operator").cast("string"))
    .withColumnRenamed("drilling_permit_number", "des_drilling_permit_number").withColumn("des_drilling_permit_number", col("des_drilling_permit_number").cast("string"))
    .withColumnRenamed("total_depth_planned", "val_total_depth_planned").withColumn("val_total_depth_planned", col("val_total_depth_planned").cast("double"))
    .withColumnRenamed("total_depth_planned_unit", "des_total_depth_planned_unit").withColumn("des_total_depth_planned_unit", col("des_total_depth_planned_unit").cast("string"))
    .withColumnRenamed("total_depth_final", "val_total_depth_final").withColumn("val_total_depth_final", col("val_total_depth_final").cast("double"))
    .withColumnRenamed("total_depth_final_unit", "des_total_depth_final_unit").withColumn("des_total_depth_final_unit", col("des_total_depth_final_unit").cast("string"))
    .withColumnRenamed("date_completed", "fec_date_completed").withColumn("fec_date_completed", col("fec_date_completed").cast("timestamp"))
    .withColumnRenamed("kind", "des_kind").withColumn("des_kind", col("des_kind").cast("string"))
    .withColumnRenamed("water_depth", "val_water_depth").withColumn("val_water_depth", col("val_water_depth").cast("double"))
    .withColumnRenamed("water_depth_unit", "des_water_depth_unit").withColumn("des_water_depth_unit", col("des_water_depth_unit").cast("string"))
    .withColumnRenamed("water_depth_datum", "des_water_depth_datum").withColumn("des_water_depth_datum", col("des_water_depth_datum").cast("string"))
    .withColumnRenamed("ground_elevation", "val_ground_elevation").withColumn("val_ground_elevation", col("val_ground_elevation").cast("double"))
    .withColumnRenamed("ground_elevation_unit", "des_ground_elevation_unit").withColumn("des_ground_elevation_unit", col("des_ground_elevation_unit").cast("string"))
    .withColumnRenamed("formation_at_td", "des_target_formation").withColumn("des_target_formation", col("des_target_formation").cast("string"))
    .withColumnRenamed("rig_days", "num_rig_days").withColumn("num_rig_days", col("num_rig_days").cast("double"))
    .withColumnRenamed("current_status", "des_current_status").withColumn("des_current_status", col("des_current_status").cast("string"))
    .withColumnRenamed("current_status_date", "fec_current_status_date").withColumn("fec_current_status_date", col("fec_current_status_date").cast("timestamp"))
    .withColumnRenamed("wellbore_fluid", "des_wellbore_fluid").withColumn("des_wellbore_fluid", col("des_wellbore_fluid").cast("string"))
    .withColumnRenamed("field_identifier", "cod_field").withColumn("cod_field", col("cod_field").cast("string"))
    .withColumnRenamed("field_name", "des_field_name").withColumn("des_field_name", col("des_field_name").cast("string"))
    .withColumnRenamed("field_type", "des_field_type").withColumn("des_field_type", col("des_field_type").cast("string"))
    .withColumnRenamed("initial_purpose", "des_initial_purpose").withColumn("des_initial_purpose", col("des_initial_purpose").cast("string"))
    .withColumnRenamed("current_purpose", "des_current_purpose").withColumn("des_current_purpose", col("des_current_purpose").cast("string"))
    .withColumnRenamed("structure", "des_play_type").withColumn("des_play_type", col("des_play_type").cast("string"))
    .withColumnRenamed("concession_identifier", "des_lease_contract_id").withColumn("des_lease_contract_id", col("des_lease_contract_id").cast("string"))
    .withColumnRenamed("description", "des_description").withColumn("des_description", col("des_description").cast("string"))
    .withColumnRenamed("create_date", "fec_create").withColumn("fec_create", col("fec_create").cast("timestamp"))
    .withColumnRenamed("create_user_id", "cod_create_user").withColumn("cod_create_user", col("cod_create_user").cast("string"))
    .withColumnRenamed("update_date", "fec_update").withColumn("fec_update", col("fec_update").cast("timestamp"))
    .withColumnRenamed("update_user_id", "cod_update_user").withColumn("cod_update_user", col("cod_update_user").cast("string"))
    .withColumnRenamed("data_source", "des_data_source").withColumn("des_data_source", col("des_data_source").cast("string"))
    .withColumnRenamed("depth_reference_kind", "des_depth_reference_kind").withColumn("des_depth_reference_kind", col("des_depth_reference_kind").cast("string"))
    .withColumnRenamed("depth_reference_datum", "des_elevation_datum").withColumn("des_elevation_datum", col("des_elevation_datum").cast("string"))
    .withColumnRenamed("depth_reference_elevation", "val_elevation").withColumn("val_elevation", col("val_elevation").cast("double"))
    .withColumnRenamed("depth_reference_elevation_unit", "des_depth_elevation_unit").withColumn("des_depth_elevation_unit", col("des_depth_elevation_unit").cast("string"))
    .withColumnRenamed("depth_reference_point", "des_elevation_type").withColumn("des_elevation_type", col("des_elevation_type").cast("string"))
    .withColumnRenamed("common_name", "des_common_name").withColumn("des_common_name", col("des_common_name").cast("string"))
    

  val columnasFinales = Seq(
    "cod_uwbi", "cod_uwi", "num_wellbore", "cod_alt_uwi_wellbore", "des_wellbore_name",
    "des_country_name", "cod_parent_uwi", "des_trajectory_shape", "val_kickoff_depth",
    "des_kickoff_depth_unit", "des_original_operator", "des_drilling_operator",
    "des_drilling_permit_number", "val_total_depth_planned", "des_total_depth_planned_unit",
    "val_total_depth_final", "des_total_depth_final_unit", "fec_date_completed",
    "des_kind", "val_water_depth", "des_water_depth_unit", "des_water_depth_datum",
    "val_ground_elevation", "des_ground_elevation_unit", "des_target_formation",
    "num_rig_days", "des_current_status", "fec_current_status_date", "des_wellbore_fluid",
    "cod_field", "des_field_name", "des_field_type", "des_initial_purpose",
    "des_current_purpose", "des_play_type", "des_lease_contract_id", "des_description",
    "fec_create", "cod_create_user", "fec_update", "cod_update_user", "des_data_source", "des_depth_reference_kind", "des_elevation_datum",
    "val_elevation", "des_depth_elevation_unit", "des_elevation_type", "des_common_name"
  )

  dfTrn.select(columnasFinales.map(col): _*)
}



def transformWellbBot(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("unique_wellbore_identifier", "cod_uwbi").withColumn("cod_uwbi", col("cod_uwbi").cast("string"))
    .withColumnRenamed("unique_well_identifier", "cod_uwi").withColumn("cod_uwi", col("cod_uwi").cast("string"))
    .withColumnRenamed("easting", "val_wellbore_bh_easting").withColumn("val_wellbore_bh_easting", col("val_wellbore_bh_easting").cast("double"))
    .withColumnRenamed("northing", "val_wellbore_bh_northing").withColumn("val_wellbore_bh_northing", col("val_wellbore_bh_northing").cast("double"))
    .withColumnRenamed("projected_coordinate_unit", "des_bh_proj_coord_unit").withColumn("des_bh_proj_coord_unit", col("des_bh_proj_coord_unit").cast("string"))
    .withColumnRenamed("projected_coordinate_system", "des_bh_proj_coord_sys").withColumn("des_bh_proj_coord_sys", col("des_bh_proj_coord_sys").cast("string"))
    .withColumnRenamed("latitude", "val_wellbore_bh_latitude").withColumn("val_wellbore_bh_latitude", col("val_wellbore_bh_latitude").cast("double"))
    .withColumnRenamed("longitude", "val_wellbore_bh_longitude").withColumn("val_wellbore_bh_longitude", col("val_wellbore_bh_longitude").cast("double"))
    .withColumnRenamed("geographic_coordinate_system", "des_wellbore_bh_geo_coord_sys").withColumn("des_wellbore_bh_geo_coord_sys", col("des_wellbore_bh_geo_coord_sys").cast("string"))
    .withColumn("cod_uwbi", trim(regexp_replace(col("cod_uwbi"), "\\s+", "")))
    .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
    .drop("des_wellbore")
    .drop("create_date")
    .drop("create_user_id")
    .drop("update_date")
    .drop("update_user_id")
    .drop("description")
    .drop("data_source")

  dfTrn
}

def transformWellbDate(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("unique_wellbore_identifier", "cod_uwbi")
    .withColumnRenamed("unique_well_identifier", "cod_uwi")
    .withColumnRenamed("date_type", "des_date_type")
    .withColumnRenamed("date_value", "fec_date_val")
    .withColumn("fec_date_val", col("fec_date_val").cast("timestamp"))
    .withColumn("cod_uwbi", trim(regexp_replace(col("cod_uwbi"), "\\s+", "")))
    .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
    .drop("create_date")
    .drop("create_user_id")
    .drop("update_date")
    .drop("update_user_id")
    .drop("description")
    .drop("data_source")
    .drop("wellbore_name")

  dfTrn
}

def transformFieldWellb(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("unique_wellbore_identifier", "cod_uwbi").withColumn("cod_uwbi", col("cod_uwbi").cast("string"))
    .withColumnRenamed("unique_well_identifier", "cod_uwi").withColumn("cod_uwi", col("cod_uwi").cast("string"))
    .withColumnRenamed("field_identifier", "cod_field").withColumn("cod_field", col("cod_field").cast("string"))
    .withColumnRenamed("field_name", "des_field").withColumn("des_field", col("des_field").cast("string"))
    .withColumnRenamed("field_type", "des_field_type").withColumn("des_field_type", col("des_field_type").cast("string"))
    .withColumn("cod_uwbi", trim(regexp_replace(col("cod_uwbi"), "\\s+", "")))
    .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
    .drop("create_date")
    .drop("create_user_id")
    .drop("update_date")
    .drop("update_user_id")
    .drop("description")
    .drop("data_source")
    .drop("wellbore_name")
    .drop("cod_field")
    .drop("des_field")
    .drop("des_field_type")

  dfTrn
}

def transformationDfWell(df: DataFrame): DataFrame = {

  val dfTrn = df
    .withColumnRenamed("unique_well_identifier", "cod_uwi")
    .withColumnRenamed("basin_identifier", "des_basin_identifier")
    .withColumnRenamed("preferred_geographic_coordinate_system", "des_preferred_geographic_coordinate_system")
    .withColumnRenamed("preferred_projected_coordinate_system", "des_preferred_projected_coordinate_system")
    .withColumnRenamed("preferred_latitude", "val_preferred_latitude")
    .withColumnRenamed("preferred_longitude", "val_preferred_longitude")
    .withColumnRenamed("well_name", "des_well_name")
    .withColumnRenamed("preferred_easting", "val_preferred_easting")
    .withColumnRenamed("preferred_northing", "val_preferred_northing")
    .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
    .select(
      "cod_uwi",
      "des_basin_identifier",
      "des_preferred_geographic_coordinate_system",
      "des_preferred_projected_coordinate_system",
      "val_preferred_latitude",
      "val_preferred_longitude",
      "des_well_name",
      "val_preferred_easting",
      "val_preferred_northing"
    )

  dfTrn
}

def transformWellbDepthInfo(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("unique_wellbore_identifier", "cod_uwbi")
    .withColumnRenamed("unique_well_identifier", "cod_uwi")
    .withColumn("cod_uwbi", trim(regexp_replace(col("cod_uwbi"), "\\s+", "")))
    .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
    .drop("create_date")
    .drop("create_user_id")
    .drop("update_date")
    .drop("update_user_id")
    .drop("description")
    .drop("data_source")
    .drop("wellbore_name")

  dfTrn
}


def joinWb(df1: DataFrame, df2: DataFrame, joinType: String = "inner"): DataFrame = {
  val dfJoined = df1.join(
    df2,
    df1("cod_uwbi") === df2("cod_uwbi") && df1("cod_uwi") === df2("cod_uwi"),
    joinType
  )

  val dfClean = dfJoined
    .drop(df2("cod_uwbi"))
    .drop(df2("cod_uwi"))

  dfClean
}

def joinWell(df1: DataFrame, df2: DataFrame, joinType: String = "inner"): DataFrame = {
  val dfJoined = df1.join(
    df2,
    df1("cod_uwi") === df2("cod_uwi"),
    joinType
  )

  val dfClean = dfJoined
    .drop(df2("cod_uwi"))

  dfClean
}

def castDouble(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    
    .withColumn("num_borehole", col("num_borehole").cast("Double"))
    .withColumn("val_norm_latitude", col("val_norm_latitude").cast("Double"))
    .withColumn("val_norm_longitude", col("val_norm_longitude").cast("Double"))
    .withColumn("val_pref_latitude", col("val_pref_latitude").cast("Double"))
    .withColumn("val_pref_longitude", col("val_pref_longitude").cast("Double"))
    .withColumn("num_rig_days", col("num_rig_days").cast("Double"))
    .withColumn("num_symbol", col("num_symbol").cast("Double"))
    .withColumn("val_easting", col("val_easting").cast("Double"))
    .withColumn("val_northing", col("val_northing").cast("Double"))
    .withColumn("val_latitude", col("val_latitude").cast("Double"))
    .withColumn("val_longitude", col("val_longitude").cast("Double"))
    .withColumn("val_x_lon", col("val_x_lon").cast("Double"))
    .withColumn("val_y_lat", col("val_y_lat").cast("Double"))
    .withColumn("num_crs_id", col("num_crs_id").cast("Double"))
    .withColumn("num_wellbore", col("num_wellbore").cast("Double"))
    .withColumn("num_authorization", col("num_authorization").cast("Double"))
    .withColumn("num_drilling_permit", col("num_drilling_permit").cast("Double"))

  dfTrn
}

def transformationDfWellAlias(df: DataFrame): DataFrame = {
  val dfTrn = df
    .filter(col("well_name_set") === "government identifier")
    .withColumn("des_gov_identifier", regexp_replace(col("alias"), "[^A-Za-z0-9]", ""))
    .withColumn("cod_uwi", trim(regexp_replace(col("unique_well_identifier"), "\\s+", "")))
    .select("cod_uwi", "des_gov_identifier")

  dfTrn
}

// COMMAND ----------

def countDuplicates(df: DataFrame): Long = {
  val totalRows = df.count()
  val distinctRows = df.distinct().count()
  val duplicates = totalRows - distinctRows
  duplicates
}

def getDuplicateRows(df: DataFrame, cols: Seq[String]): DataFrame = {
  val duplicatesKeysDf = df
    .groupBy(cols.map(col): _*)
    .count()
    .filter(col("count") > 1)
    .drop("count")

  val duplicatesDf = df.join(duplicatesKeysDf, cols, "inner")
  duplicatesDf
}

// COMMAND ----------


def mostrarValoresUnicos(df: DataFrame, columna: String): Unit = {
  df.select(columna)
    .distinct()
    .orderBy(columna)
    .show(false)
}


// COMMAND ----------

def addDateColumns(df: DataFrame): DataFrame = {
  df.withColumn("fec_proposed_drill_start",
        when(col("des_date_type") === "PROPOSED DRILLING START", to_timestamp(col("fec_date_val"))))
    .withColumn("fec_proposed_drill_finish",
        when(col("des_date_type") === "PROPOSED DRILLING FINISH", to_timestamp(col("fec_date_val"))))
    .withColumn("fec_drill_start",
        when(col("des_date_type") === "DRILLING STARTED", to_timestamp(col("fec_date_val"))))
    .withColumn("fec_drill_finished",
        when(col("des_date_type") === "DRILLING FINISHED", to_timestamp(col("fec_date_val"))))
    .withColumn("fec_rig_release",
        when(col("des_date_type") === "RIG RELEASE", to_timestamp(col("fec_date_val"))))
    .withColumn("fec_completion",
        when(col("des_date_type") === "COMPLETION", to_timestamp(col("fec_date_val"))))
    .withColumn("fec_spud",
        when(col("des_date_type") === "SPUD", to_timestamp(col("fec_date_val"))))
    .withColumn("fec_drilling_permit",
        when(col("des_date_type") === "DRILLING PERMIT", to_timestamp(col("fec_date_val"))))
}


// COMMAND ----------

import java.sql.Timestamp

def nullifyOutOfRangeDates(df: DataFrame): DataFrame = {
  val lowerBound = Timestamp.valueOf("1753-01-01 00:00:00")
  val upperBound = Timestamp.valueOf("9999-12-31 23:59:59")

  // 📝 Lista de columnas a limpiar (puedes modificarla aquí)
  val targetCols = Seq(
  "fec_current_status_date",
  "fec_date_completed",
  "fec_create",
  "fec_update",
  "fec_date_val",
  "fec_proposed_drill_start",
  "fec_proposed_drill_finish",
  "fec_drill_start",
  "fec_drill_finished",
  "fec_rig_release",
  "fec_completion",
  "fec_spud",
  "fec_drilling_permit"
)

  var cleanedDF = df

  targetCols.foreach { colName =>
    if (df.columns.contains(colName)) {
      val outOfRangeCount = df
        .filter(col(colName).isNotNull)
        .filter(col(colName) < lit(lowerBound) || col(colName) > lit(upperBound))
        .count()

      if (outOfRangeCount > 0) {
        
        cleanedDF = cleanedDF.withColumn(
          colName,
          when(col(colName).between(lowerBound, upperBound), col(colName)).otherwise(lit(null))
        )
      }
    } else {

    }
  }

  cleanedDF
}


// COMMAND ----------

def addTotalDepthColumns(df: DataFrame): DataFrame = {
  val tdType = upper(trim(col("total_depth_type"))) // normaliza por si vienen espacios o minúsculas

  df
    // Proposed Total Depth (TVD)
    .withColumn(
      "val_proposed_total_depth_tvd",
      when(tdType === lit("PLAN_TVD"), col("total_depth").cast("double"))
        .otherwise(lit(null).cast("double"))
    )
    .withColumn(
      "des_proposed_total_depth_tvd_unit",
      when(tdType === lit("PLAN_TVD"), col("total_depth_unit").cast("string"))
        .otherwise(lit(null).cast("string"))
    )
    // Final Total Depth (TVD)
    .withColumn(
      "val_final_total_depth_tvd",
      when(tdType === lit("TVD"), col("total_depth").cast("double"))
        .otherwise(lit(null).cast("double"))
    )
    .withColumn(
      "des_final_total_depth_tvd_unit",
      when(tdType === lit("TVD"), col("total_depth_unit").cast("string"))
        .otherwise(lit(null).cast("string"))
    )
}


// COMMAND ----------

var dlInsert = "N/A"
var dlMsg = ""

// var df: DataFrame  = null  // AS ALREADY DEFINED AS VAL BELOW.

val dfWellbore = transformDfWellbore(getDataFromSource(sourcesFinal("aa001163_glb_cds_wellbore")))

val dfWellbDate = transformWellbDate(getDataFromSource(sourcesFinal("aa001163_glb_cds_wellbore_date")))

val dfWellbBot = transformWellbBot(getDataFromSource(sourcesFinal("aa001163_glb_cds_wellbore_bottom_hole"))) 

val dfFieldWellb = transformFieldWellb(getDataFromSource(sourcesFinal("aa001163_glb_cds_field_wellbore")))

val dfWell = transformationDfWell(getDataFromSource(sourcesFinal("aa001163_glb_cds_well")))

val dfWellAlias = transformationDfWellAlias(getDataFromSource(sourcesFinal("aa001163_glb_cds_well_alias")))

val dfWellDepthInfo = transformWellbDepthInfo(getDataFromSource(sourcesFinal("aa001163_glb_cds_wb_total_depth_information")))

val df= joinWb(dfWellbore, dfWellbBot, "left")

val df2 = joinWb(df, dfWellbDate, "left")

val df3 = joinWb(df2, dfFieldWellb, "left")

val df4 = joinWell(df3, dfWell, "left")

val df5 = joinWell(df4, dfWellAlias, "left")

val df6 = joinWb(df5, dfWellDepthInfo, "left")

val df7 = addDateColumns(df6)

val df8 = addTotalDepthColumns(df7)

val df9 = nullifyOutOfRangeDates(df8)
// df9.printSchema()
val dfFinal = df9.select(orderFields.map(col): _*)
// dfFinal.printSchema()
def toScalaMap(obj: Any): Map[String, String] = obj match {
        case m: java.util.Map[_, _] => m.asScala.toMap.map { case (k, v) => k.toString -> v.toString }
        case m: Map[_, _] => m.map { case (k, v) => k.toString -> v.toString }
    }
val insertFields = auxParamsFinal.get("insert_fields").map(toScalaMap).getOrElse(Map.empty[String, String])
val finalDf = addMissingColumns(dfFinal, insertFields)
val dfFormatted = applyDDLFromTableToDF(finalDf,getObject((target_object).toString))
// dfFormatted.printSchema()
writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

// dlInsert = "OK"

// println(dlInsert)
// println(dlMsg)

// COMMAND ----------

val result = s"""{"error": "$error"}"""

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

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

val orderFields: Seq[String] = Seq("cod_field", "des_field", "des_country", "des_state_province", "des_field_type", "cod_field_code", "num_field_number", "des_primary_reservoir", "cod_owner", "des_curr_operator", "des_curr_status", 
"des_producing_status", "cod_parent", "des_parent_name", "des_parent_type", "des_nearest_discovery", "des_supply_base", "ind_multi_reservoir", "des_prospect", "fec_drill_start", "fec_drill_end", "cod_notion_wb", "cod_notion_w", "des_notion_wb", 
"des_fluid_type", "des_disc_basis", "cod_disc_wb", "cod_disc_w", "des_disc_wb", "fec_discovery", "des_onshore_offshore", "fec_first_prod", "fec_last_prod", "cod_basin", "des_basin", "des_basin_abbr", "des_basin_type", "des_play", "des_trend", 
"val_drill_prob", "val_latitude", "val_longitude", "des_geo_coord_sys", "val_easting", "val_northing", "des_coord_unit", "des_coord_sys", "val_dist_shore", "des_dist_unit", "val_pipe_dist", "val_facility_dist", "val_water_depth", "des_water_depth_unit", 
"val_est_dev_cost", "val_act_dev_cost", "des_currency", "fec_create", "cod_create_user", "fec_update", "cod_update_user")



def transformField(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("identifier", "cod_field")
    .withColumnRenamed("name", "des_field")
    .withColumnRenamed("country_name", "des_country")
    .withColumnRenamed("state_or_province", "des_state_province")
    .withColumnRenamed("field_type", "des_field_type")
    .withColumnRenamed("field_code", "cod_field_code")
    .withColumnRenamed("field_number", "num_field_number").withColumn("num_field_number", col("num_field_number").cast("integer"))
    .withColumnRenamed("primary_reservoir", "des_primary_reservoir")
    .withColumnRenamed("owner_identifier", "cod_owner")
    .withColumnRenamed("current_operator", "des_curr_operator")
    .withColumnRenamed("current_status", "des_curr_status")
    .withColumnRenamed("producing_status", "des_producing_status")
    .withColumnRenamed("parent_identifier", "cod_parent")
    .withColumnRenamed("parent_name", "des_parent_name")
    .withColumnRenamed("parent_type", "des_parent_type")
    .withColumnRenamed("nearest_discovery", "des_nearest_discovery")
    .withColumnRenamed("supply_base_name", "des_supply_base")
    .withColumnRenamed("multiple_reservoir_indicator", "ind_multi_reservoir")
    .withColumnRenamed("prospect_description", "des_prospect")
    .withColumnRenamed("drilling_start_date", "fec_drill_start").withColumn("fec_drill_start", col("fec_drill_start").cast("timestamp"))
    .withColumnRenamed("drilling_end_date", "fec_drill_end").withColumn("fec_drill_end", col("fec_drill_end").cast("timestamp"))
    .withColumnRenamed("notional_wellbore_identifier", "cod_notion_wb")
    .withColumnRenamed("notional_well_identifier", "cod_notion_w")
    .withColumnRenamed("notional_wellbore_name", "des_notion_wb")
    .withColumnRenamed("fluid_type", "des_fluid_type")
    .withColumnRenamed("discovery_basis", "des_disc_basis")
    .withColumnRenamed("discovery_wellbore_identifier", "cod_disc_wb")
    .withColumnRenamed("discovery_well_identifier", "cod_disc_w")
    .withColumnRenamed("discovery_wellbore_name", "des_disc_wb")
    .withColumnRenamed("discovery_date", "fec_discovery").withColumn("fec_discovery", col("fec_discovery").cast("timestamp"))
    .withColumnRenamed("onshore_or_offshore", "des_onshore_offshore")
    .withColumnRenamed("first_production_date", "fec_first_prod").withColumn("fec_first_prod", col("fec_first_prod").cast("timestamp"))
    .withColumnRenamed("last_production_date", "fec_last_prod").withColumn("fec_last_prod", col("fec_last_prod").cast("timestamp"))
    .withColumnRenamed("basin_identifier", "cod_basin")
    .withColumnRenamed("basin_name", "des_basin")
    .withColumnRenamed("basin_abbreviation", "des_basin_abbr")
    .withColumnRenamed("basin_type", "des_basin_type")
    .withColumnRenamed("play_name", "des_play")
    .withColumnRenamed("trend", "des_trend")
    .withColumnRenamed("drilling_probability", "val_drill_prob").withColumn("val_drill_prob", col("val_drill_prob").cast("Double"))
    .withColumnRenamed("latitude", "val_latitude").withColumn("val_latitude", col("val_latitude").cast("double"))
    .withColumnRenamed("longitude", "val_longitude").withColumn("val_longitude", col("val_longitude").cast("double"))
    .withColumnRenamed("geographic_coordinate_system", "des_geo_coord_sys")
    .withColumnRenamed("easting", "val_easting").withColumn("val_easting", col("val_easting").cast("double"))
    .withColumnRenamed("northing", "val_northing").withColumn("val_northing", col("val_northing").cast("double"))
    .withColumnRenamed("original_coordinate_unit", "des_coord_unit")
    .withColumnRenamed("original_coordinate_system", "des_coord_sys")
    .withColumnRenamed("distance_from_shore", "val_dist_shore").withColumn("val_dist_shore", col("val_dist_shore").cast("Double"))
    .withColumnRenamed("distance_unit", "des_dist_unit")
    .withColumnRenamed("pipeline_distance", "val_pipe_dist").withColumn("val_pipe_dist", col("val_pipe_dist").cast("Double"))
    .withColumnRenamed("facility_distance", "val_facility_dist").withColumn("val_facility_dist", col("val_facility_dist").cast("Double"))
    .withColumnRenamed("water_depth", "val_water_depth").withColumn("val_water_depth", col("val_water_depth").cast("Double"))
    .withColumnRenamed("water_depth_unit", "des_water_depth_unit")
    .withColumnRenamed("estimated_development_cost", "val_est_dev_cost").withColumn("val_est_dev_cost", col("val_est_dev_cost").cast("Double"))
    .withColumnRenamed("actual_development_cost", "val_act_dev_cost").withColumn("val_act_dev_cost", col("val_act_dev_cost").cast("Double"))
    .withColumnRenamed("currency", "des_currency")
    .withColumnRenamed("create_date", "fec_create").withColumn("fec_create", col("fec_create").cast("timestamp"))
    .withColumnRenamed("create_user_id", "cod_create_user")
    .withColumnRenamed("update_date", "fec_update").withColumn("fec_update", col("fec_update").cast("timestamp"))
    .withColumnRenamed("update_user_id", "cod_update_user")
    .drop("description")
    .drop("data_source")

  dfTrn
}


def transformFieldRespOrg(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("field_identifier", "cod_field")
    .withColumnRenamed("field_name", "des_field")
    .withColumnRenamed("country_name", "des_country")
    .withColumnRenamed("field_type", "des_field_type")
    .withColumnRenamed("organization_identifier", "cod_org")
    .withColumnRenamed("organization_long_id", "cod_org_long")
    .withColumnRenamed("organization_kind", "des_org_kind")
    .withColumnRenamed("start_date", "fec_start").withColumn("fec_start", col("fec_start").cast("timestamp"))
    .withColumnRenamed("end_date", "fec_end").withColumn("fec_end", col("fec_end").cast("timestamp"))
    .withColumnRenamed("create_date", "fec_create").withColumn("fec_create", col("fec_create").cast("timestamp"))
    .withColumnRenamed("create_user_id", "cod_create_user")
    .withColumnRenamed("update_date", "fec_update").withColumn("fec_update", col("fec_update").cast("timestamp"))
    .withColumnRenamed("update_user_id", "cod_update_user")
    .drop("cod_update_user")
    .drop("cod_create_user")
    .drop("fec_update")
    .drop("fec_create")
    .drop("des_field_type")
    .drop("des_country")
    .drop("des_field")
    .drop("description")
    .drop("data_source")

  dfTrn
}

def transformInstFieldAssoc(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("installation_identifier", "cod_installation")
    .withColumnRenamed("installation_name", "des_installation")
    .withColumnRenamed("installation_type", "des_installation_type")
    .withColumnRenamed("field_identifier", "cod_field")
    .withColumnRenamed("field_name", "des_field")
    .withColumnRenamed("country_name", "des_country")
    .withColumnRenamed("field_type", "des_field_type")
    .withColumnRenamed("start_date", "fec_start").withColumn("fec_start", col("fec_start").cast("timestamp"))
    .withColumnRenamed("end_date", "fec_end").withColumn("fec_end", col("fec_end").cast("timestamp"))
    .withColumnRenamed("create_date", "fec_create").withColumn("fec_create", col("fec_create").cast("timestamp"))
    .withColumnRenamed("create_user_id", "cod_create_user")
    .withColumnRenamed("update_date", "fec_update").withColumn("fec_update", col("fec_update").cast("timestamp"))
    .withColumnRenamed("update_user_id", "cod_update_user")
    .drop("cod_update_user")
    .drop("cod_create_user")
    .drop("fec_update")
    .drop("fec_create")
    .drop("des_field_type")
    .drop("des_field")
    .drop("des_country")
    .drop("description")
    .drop("data_source")
    .drop("fec_start")
    .drop("fec_end")

  dfTrn
}

def transformFieldWellbore(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("field_identifier", "cod_field")
    .withColumnRenamed("field_name", "des_field")
    .withColumnRenamed("field_type", "des_field_type")
    .withColumnRenamed("unique_wellbore_identifier", "cod_uwbi")
    .withColumnRenamed("unique_well_identifier", "cod_uwi")
    .withColumnRenamed("wellbore_name", "des_wellbore")
    .withColumnRenamed("start_date", "fec_start").withColumn("fec_start", col("fec_start").cast("timestamp"))
    .withColumnRenamed("end_date", "fec_end").withColumn("fec_end", col("fec_end").cast("timestamp"))
    .withColumnRenamed("create_date", "fec_create").withColumn("fec_create", col("fec_create").cast("timestamp"))
    .withColumnRenamed("create_user_id", "cod_create_user")
    .withColumnRenamed("update_date", "fec_update").withColumn("fec_update", col("fec_update").cast("timestamp"))
    .withColumnRenamed("update_user_id", "cod_update_user")
    .drop("description")
    .drop("data_source")

  dfTrn
}


def joinField(df1: DataFrame, df2: DataFrame, joinType: String = "inner"): DataFrame = {
  val dfJoined = df1.join(
    df2,
    df1("cod_field") === df2("cod_field"),
    joinType
  )
  val dfClean = dfJoined.drop(df2("cod_field"))
  dfClean
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

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {   

        val dfField = transformField(getDataFromSource(sourcesFinal("aa001163_glb_cds_field")))  

        val dfFieldRespOrg = transformFieldRespOrg(getDataFromSource(sourcesFinal("aa001163_glb_cds_field_resp_org")))

        val dfInstFieldAssoc = transformInstFieldAssoc(getDataFromSource(sourcesFinal("aa001163_glb_cds_inst_field_assoc")))

        val dfFieldWellb = getDataFromSource(sourcesFinal("aa001163_glb_cds_field_wellbore"))

        val df = joinField(dfField, dfFieldRespOrg, "left")

        val df2 = joinField(df, dfInstFieldAssoc, "left")


        val df2Final = df2.select(orderFields.map(col): _*)

        val dfFinal= df2Final.dropDuplicates("cod_field")
        dfFinal.printSchema

        val count= dfFinal.count
        println("Numero de rows:")
        println(count)

        val dfFormatted = applyDDLFromTableToDF(dfFinal,getObject((target_object).toString))
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


val result = s"""{"error": "$error"}"""

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

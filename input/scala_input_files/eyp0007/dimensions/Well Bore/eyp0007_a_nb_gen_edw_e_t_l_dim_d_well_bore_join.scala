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

val order = Seq("id_vers", "id_well_bore", "des_well_bore", "id_uwi", "id_cds", "des_wellbore_cds", "fec_start_date", 
"fec_end_date", "id_vers_well","id_well", "des_country", "ind_east_utm_td", "ind_north_utm_td", "ind_rkb_depth", "ind_sidetrack_depth", 
"ind_tvdss_depth", "ind_borehole_number", "des_common_name", "des_current_purpose", "fec_completed_date", 
"des_depth_ref_datum", "ind_depth_ref_elev", "des_depth_ref_elev_unit", "des_depth_ref_point", "des_drilling_operator", 
"des_drilling_permit", "id_field", "des_field", "ind_ground_elev", "des_ground_elev_unit", "des_innitial_purpose", 
"ind_kickoff_depth", "des_kickoff_depth_unit", "des_kind", "des_norm_bh_geo_coord_sys", 
"ind_normalized_bh_latitude", "ind_normalized_bh_longitude", "des_original_operator", "id_cds_parent", 
"des_pref_bh_geo_coord_sys", "ind_preferred_bh_latitude", "ind_preferred_bh_longitude", 
"des_pref_bh_prj_coord_sys", "des_pref_bh_prj_coord_unit", "ind_preffered_bh_easting", 
"ind_preffered_bh_northing", "fec_spud_date", "ind_total_depth_final", "des_total_depth_final_unit", 
"ind_total_depth_planned", "des_total_depth_planned_unit", "des_trajectory_shape", "fec_create_date", "fec_update_date")


val rowDummy = Row("-99", "-99", null, null, null, null, java.sql.Timestamp.valueOf("1900-01-01 00:00:00"), null, "-99", null, null,  
null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
 null, null, null, null, null, null, null) 


def addRowDummy(df: DataFrame): DataFrame = {
    
    var dfTrn: DataFrame = df

    val dfDummy = spark.createDataFrame(Seq(rowDummy).asJava, df.schema)

    dfTrn = dfTrn.union(dfDummy)

    return dfTrn

}

def addIdVers (df: DataFrame): DataFrame = {
    
   var dfTrn: DataFrame = df

   dfTrn = dfTrn.withColumn("id_vers_mod", when(col("id_vers_well") === "-99", concat(lit("19000101"), col("id_vers")))
                                            .otherwise(concat(col("id_vers_well").substr(0,8), col("id_vers"))))
                                            .drop("id_vers")
                                            .withColumnRenamed("id_vers_mod", "id_vers")

   return dfTrn
    
}

def transformationDfWell (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("unique_wellbore_identifier", "id_cds")
                .withColumnRenamed("alternate_uwi", "id_uwi")//cambiado por el de arriba id_cds por id_uwi
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
                .withColumnRenamed("normalized_bh_latitude", "ind_normalized_bh_latitude")
                .withColumnRenamed("normalized_bh_longitude", "ind_normalized_bh_longitude")
                .withColumnRenamed("original_operator", "des_original_operator")
                .withColumnRenamed("parent_wellbore_identifier", "id_cds_parent")
                .withColumnRenamed("preferred_bh_geographic_coordinate_sys", "des_pref_bh_geo_coord_sys")
                .withColumnRenamed("preferred_bh_latitude", "ind_preferred_bh_latitude")
                .withColumnRenamed("preferred_bh_longitude", "ind_preferred_bh_longitude")
                .withColumnRenamed("preferred_bh_projected_coordinate_system", "des_pref_bh_prj_coord_sys")
                .withColumnRenamed("preferred_bh_projected_coordinate_unit", "des_pref_bh_prj_coord_unit")
                .withColumnRenamed("preferred_bh_easting", "ind_preffered_bh_easting")
                .withColumnRenamed("preferred_bh_northing", "ind_preffered_bh_northing")
                .withColumnRenamed("spud_date", "fec_spud_date")
                .withColumnRenamed("total_depth_final", "ind_total_depth_final")
                .withColumnRenamed("total_depth_final_unit", "des_total_depth_final_unit")
                .withColumnRenamed("total_depth_planned", "ind_total_depth_planned")
                .withColumnRenamed("total_depth_planned_unit", "des_total_depth_planned_unit")
                .withColumnRenamed("trajectory_shape", "des_trajectory_shape")
                //.withColumnRenamed("alternate_uwi", "join_col_well_bore")
                .withColumnRenamed("update_date", "dateCDS")
   
    return dfTrn

}



def joinDfWellAndWellHole (dfL: DataFrame, dfR: DataFrame): DataFrame = {

    val hWin = Window.partitionBy(col("id_well_bore"), col("fec_start_date")).orderBy(col("dateCDS").desc)

    // var resDf = dfL.join(dfR, dfL("master_sys_code") === dfR("id_cds") && dfR("dateCDS") <= dfL("fec_start_date"), "left")
    var resDf = dfL.join(dfR, dfL("master_sys_code") === dfR("id_cds"), "left")

    return resDf.withColumn("new", row_number().over(hWin) ).where(col("new") === 1)
}


def joinWellboreAndWell (dfL: DataFrame, dfR: DataFrame): DataFrame = {

    var resDf = dfL.join(dfR,
                        dfL("id_well") ===  dfR("id_well_2") &&
                        dfL("fec_end_date_aux") >=  dfR("fec_start_date_well") &&
                        dfL("fec_start_date") <  dfR("fec_end_date_well_aux"), 
                        "left"
                        ).withColumn("id_vers_well", coalesce(dfR("id_vers_well"), lit("-99")))

    return resDf
}

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {
        val dfWellboreCDS: DataFrame = transformationDfWell(getDataFromSource(sourcesFinal("aa001163_glb_wellbore")))
        val dfWellBore: DataFrame = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_dim_d_well_bore")).filter(col("id_vers") =!= "-99")
                                                                            .withColumn("fec_end_date_aux", when(col("fec_end_date").isNull, to_timestamp(lit("9999-12-31 00:00:00")))
                                                                            .otherwise(col("fec_end_date")))

        val dfWell: DataFrame = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well")).select("id_vers", "id_well", "fec_start_date", "fec_end_date")
                                                                        .withColumnRenamed("id_vers", "id_vers_well")
                                                                        .withColumnRenamed("id_well", "id_well_2")
                                                                        .withColumnRenamed("fec_start_date", "fec_start_date_well")
                                                                        .withColumnRenamed("fec_end_date", "fec_end_date_well")
                                                                        .withColumn("fec_end_date_well_aux", when(col("fec_end_date_well").isNull, to_timestamp(lit("9999-12-31 00:00:00")))
                                                                        .otherwise(col("fec_end_date_well")))


        df = joinDfWellAndWellHole(dfWellBore, dfWellboreCDS)

        df = joinWellboreAndWell(df, dfWell)

        df = addIdVers(df)

        df = df.select(order.map(col): _*)

        df = addRowDummy(df)   
        
    val dfFormatted = applyDDLFromTableToDF(df,getObject(targetFinal("target_object").toString))
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
// MAGIC

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

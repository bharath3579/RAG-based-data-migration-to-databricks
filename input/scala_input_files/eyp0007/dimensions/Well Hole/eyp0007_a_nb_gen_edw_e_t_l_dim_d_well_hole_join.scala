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


val order = Seq("id_vers", "id_well_hole", "des_well_hole", "id_cds", "des_cds", "fec_start_date", "fec_end_date", "ind_bh_latitude", "ind_bh_longitude", "des_country", "ind_latitude", "ind_longitude", "id_fcty_1", "des_status", "id_basin", "des_common_name", "des_county", "des_country_name", "des_ground_elevation", "des_ground_elevation_unit", "des_normalized_geographic_coordinate_system", "des_onshore_or_offshore", "des_slot_name", "fec_spud_date", "des_state_or_province", "des_well_name", "ind_normalized_latitud", "ind_normalized_longitude", "fec_create_date", "fec_update_date")


def transformationDfWell (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("basin_identifier", "id_basin")
                    .withColumnRenamed("common_name", "des_common_name")
                    .withColumnRenamed("county", "des_county")
                    .withColumnRenamed("country_name", "des_country_name")
                    .withColumnRenamed("ground_elevation", "des_ground_elevation")
                    .withColumnRenamed("ground_elevation_unit", "des_ground_elevation_unit")
                    .withColumnRenamed("normalized_geographic_coordinate_system", "des_normalized_geographic_coordinate_system")
                    .withColumnRenamed("onshore_or_offshore", "des_onshore_or_offshore")
                    .withColumnRenamed("slot_name", "des_slot_name")
                    .withColumnRenamed("spud_date", "fec_spud_date")
                    .withColumnRenamed("state_or_province", "des_state_or_province")
                    .withColumnRenamed("well_name", "des_well_name")
                    .withColumnRenamed("normalized_latitude", "ind_normalized_latitud")
                    .withColumnRenamed("normalized_longitude", "ind_normalized_longitude")
                    .withColumnRenamed("unique_well_identifier", "join_col_well_hole")
                    .withColumnRenamed("update_date", "dateCDS")
   
    return dfTrn

}



def joinDfWellAndWellHole (dfL: DataFrame, dfR: DataFrame): DataFrame = {

    val hWin = Window.partitionBy(col("id_vers"), col("fec_start_date")).orderBy(col("dateCDS").desc)

    // var resDf = dfL.join(dfR, dfL("id_cds") === dfR("join_col_well_hole") && dfR("dateCDS") <= dfL("fec_start_date"), "left")
    var resDf = dfL.join(dfR, dfL("id_cds") === dfR("join_col_well_hole"), "left")

    return resDf.withColumn("new", row_number().over(hWin) ).where(col("new") === 1)
}

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {
        val dfWell: DataFrame = transformationDfWell(getDataFromSource(sourcesFinal("aa001163_glb_well")))
        val dfWellHole: DataFrame = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_dim_d_well_hole"))
        
        // dfWellHole(master_sys_code) - left - dfWell(unique_well_identifier)
        df = joinDfWellAndWellHole(dfWellHole, dfWell)

        df = df.select(order.map(col): _*)
    
        
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

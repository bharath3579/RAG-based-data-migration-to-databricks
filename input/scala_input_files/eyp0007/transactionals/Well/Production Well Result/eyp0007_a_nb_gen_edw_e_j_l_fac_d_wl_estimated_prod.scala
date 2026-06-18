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
// MAGIC # Código que implementa la lógica necesaria

// COMMAND ----------

val flagFullLoad = paramsFinal.getOrElse("full_load", "false").toString.toBoolean



import org.apache.spark.sql.functions._
var dlInsert = "N/A"
var dlMsg = ""
var df: DataFrame  = null

val selectFields = Seq("fec_production_day", "id_vers_well", "id_well",
"ind_gas_rate_adj_mscfperday", "ind_gas_rate_adj_kscfperday", "ind_gas_rate_adj_sm3perday", "ind_net_cond_rate_adj_stbperday", "ind_net_cond_rate_adj_sm3perday", "ind_net_oil_rate_adj_stbperday", "ind_net_oil_rate_adj_sm3perday", "ind_tot_water_rate_adj_bblperday", "ind_tot_water_rate_adj_m3perday",
"id_commercial_entity", "id_facility_class_1", "id_well_hookup", "id_well_hole", "id_area", "id_productionunit", "id_geo_area", "id_field_cds", "id_field", "id_basin", "id_operator_route", "id_col_point", "id_licence", "fec_end_date", "fec_valid_from", "ind_duration", "des_status", "ind_result_no", "fec_create_date", "fec_update_date")

df = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_wl_prod_res")).where(col("des_status") === "Accepted" && col("id_bu_fac").isin("RNAS", "MBU", "EFD2", "BOL", "PER"))


try {

    val winSpec = Window.partitionBy("id_well").orderBy(col("fec_valid_from").asc_nulls_last, col("fec_update_date").asc)
    val actualDate = java.time.LocalDate.now

    df = df
    .withColumn("fec_valid_from", coalesce(col("fec_valid_from"), col("fec_production_day")) )
    .drop("fec_production_day")

    df = df
    // Duplicado de datos
    .withColumn("lead_date",
                    coalesce(lead("fec_valid_from", 1).over(winSpec), lit(actualDate))
    )
    .withColumn("valid_from_formatted",
                    col("fec_valid_from").cast(DateType)
    )
    .withColumn("lead_date",
                    col("lead_date").cast(DateType)
    )
    .withColumn("next_date", when(
        col("valid_from_formatted") === col("lead_date") || col("valid_from_formatted") > col("lead_date"),
        col("valid_from_formatted")
        ).otherwise(
            date_sub(col("lead_date"), 1)
        )                   
    )
    .withColumn("date_range",
                    expr("sequence(valid_from_formatted, next_date, interval 1 day)")
    )
    .withColumn("fec_production_day",
                    explode(col("date_range"))
    )
    .withColumn("fec_production_day",
                    to_timestamp(col("fec_production_day"))
    )

    df = df.select(selectFields.map(col): _*)

    val dfFormatted = applyDDLFromTableToDF(df,getObject(targetFinal("target_object").toString))
    writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

    dlMsg = "Rows Inserted: " + df.count
    dlInsert = "OK"

} catch {
    case e: Throwable => 
                error = e.toString
                println(error)
                LogHelper().logEndKO(error)
}


println(dlInsert)
println(dlMsg)



// COMMAND ----------

// MAGIC %md
// MAGIC # Informar del resultado

// COMMAND ----------

val result = s"""{"error": "$error"}"""

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)

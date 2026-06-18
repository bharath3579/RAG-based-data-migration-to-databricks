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
import org.apache.spark.sql.functions._

val order = Seq("id_vers", "id_well", "des_well", "id_cds", "des_cds", "fec_start_date", "fec_end_date", "bol_alloc_flag", "des_choke_uom", "id_commercial_entity", "id_country_ws", "des_lift_system", "des_on_stream_method", "id_fcty_1", "id_well_hookup", "bol_operator_flag", "des_pump_type", "des_well_class", "des_well_function", "id_well_hole", "des_well_main_fluid", "des_well_meter_freq", "des_ref_object", "des_type_well", "fec_create_date", "fec_update_date")

def transformationDfDimWell (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumn("daytime_aux", date_format(col("daytime"), "yyyyMMdd"))
    
    dfTrn = dfTrn.withColumn("id_vers", concat(col("daytime_aux"), col("object_id")))

    dfTrn = dfTrn.withColumnRenamed("object_id", "id_well")
                    .withColumnRenamed("name", "des_well")
                    .withColumnRenamed("master_sys_code", "id_cds")
                    .withColumnRenamed("master_sys_name", "des_cds")
                    .withColumnRenamed("daytime", "fec_start_date")
                    .withColumnRenamed("end_date", "fec_end_date")
                    .withColumnRenamed("alloc_flag", "bol_alloc_flag")
                    .withColumnRenamed("choke_uom", "des_choke_uom")
                    .withColumnRenamed("commercial_entity_id", "id_commercial_entity")
                    .withColumnRenamed("country_ws", "id_country_ws")
                    .withColumnRenamed("lift_system", "des_lift_system")
                    .withColumnRenamed("on_stream_method", "des_on_stream_method")
                    .withColumnRenamed("op_fcty_1_id", "id_fcty_1")
                    .withColumnRenamed("op_well_hookup_id", "id_well_hookup")
                    .withColumnRenamed("operator_flag", "bol_operator_flag")
                    .withColumnRenamed("pump_type", "des_pump_type")
                    .withColumnRenamed("well_class", "des_well_class")
                    .withColumnRenamed("well_function", "des_well_function")
                    .withColumnRenamed("well_hole_id", "id_well_hole")
                    .withColumnRenamed("well_main_fluid", "des_well_main_fluid")
                    .withColumnRenamed("well_meter_freq", "des_well_meter_freq")
                    .withColumnRenamed("well_reference_object", "des_ref_object")
                    .withColumnRenamed("well_type", "des_type_well")
                    .withColumnRenamed("created_date", "fec_create_date")
                    .withColumnRenamed("last_updated_date", "fec_update_date")
 
    
    dfTrn = dfTrn.select(order.map(col): _*)
   
    return dfTrn

}

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------


var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null
var df_ec_codes: DataFrame  = null

try {

        
    df = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_well")).withColumnRenamed("id_bu", "bu")

    df_ec_codes = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_ec_codes")).withColumnRenamed("id_bu", "bu_ec_codes")

    //val dfComEnt = getDataFromSource(sourcesFinal(2))

    df = df.as("well").join(
                            df_ec_codes.filter(col("code_type") === "CHOKE_TYPE").drop("created_date", "last_updated_date").dropDuplicates("code_text", "bu_ec_codes").as("ec_code"), 
                            col("well.choke_uom") === col("ec_code.code") &&
                            col("well.bu") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("choke_uom", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "choke_uom")

    df = df.as("well").join(
                            df_ec_codes.filter(col("code_type") === "LIFT_SYSTEM").drop("created_date", "last_updated_date").dropDuplicates("code_text", "bu_ec_codes").as("ec_code"), 
                            col("well.lift_system") === col("ec_code.code") &&
                            col("well.bu") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("lift_system", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "lift_system")

     df = df.as("well").join(
                            df_ec_codes.filter(col("code_type") === "ON_STREAM_METHOD").drop("created_date", "last_updated_date").dropDuplicates("code_text", "bu_ec_codes").as("ec_code"), 
                            col("well.on_stream_method") === col("ec_code.code") &&
                            col("well.bu") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("on_stream_method", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "on_stream_method")

    df = df.as("well").join(
                            df_ec_codes.filter(col("code_type") === "PUMP_TYPE").drop("created_date", "last_updated_date").dropDuplicates("code_text", "bu_ec_codes").as("ec_code"), 
                            col("well.pump_type") === col("ec_code.code") &&
                            col("well.bu") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("pump_type", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "pump_type")

    df = df.as("well").join(
                            df_ec_codes.filter(col("code_type") === "WELL_FUNCTION").drop("created_date", "last_updated_date").dropDuplicates("code_text", "bu_ec_codes").as("ec_code"), 
                            col("well.well_function") === col("ec_code.code") &&
                            col("well.bu") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("well_function", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "well_function")

    df = df.as("well").join(
                            df_ec_codes.filter(col("code_type") === "WELL_MAIN_FLUID").drop("created_date", "last_updated_date").dropDuplicates("code_text", "bu_ec_codes").as("ec_code"), 
                            col("well.well_main_fluid") === col("ec_code.code") &&
                            col("well.bu") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("well_main_fluid", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "well_main_fluid")

    df = df.as("well").join(
                            df_ec_codes.filter(col("code_type") === "METER_FREQ").drop("created_date", "last_updated_date").dropDuplicates("code_text", "bu_ec_codes").as("ec_code"), 
                            col("well.well_meter_freq") === col("ec_code.code") &&
                            col("well.bu") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("well_meter_freq", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "well_meter_freq")

    df = df.as("well").join(
                            df_ec_codes.filter(col("code_type") === "WELL_TYPE").drop("created_date", "last_updated_date").dropDuplicates("code_text", "bu_ec_codes").as("ec_code"), 
                            col("well.well_type") === col("ec_code.code") &&
                            col("well.bu") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("well_type", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "well_type")


    df = transformationDfDimWell(df)

    /*
    df = df.join(
        dfComEnt
          .select(col("id_commercial_entity"), col("id_vers").alias("cod_vers_commercial_entity")),
        Seq("id_commercial_entity"),
        "left" 
            )*/

    val count= df.count
    println(count)
    
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

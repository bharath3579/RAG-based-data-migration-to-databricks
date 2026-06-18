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

val order = Seq("id_vers", "id_eqpm", "des_eqpm", "fec_start_date", "fec_end_date", "des_type_eqpm", "id_code_pump_model", "num_pump_ser", "id_tag","des_uom","id_counter", "id_col_point", "id_fcty_1", "fec_create_date", "fec_update_date")

val rowDummy = Row("-99", "-99", "Not available", java.sql.Timestamp.valueOf("1900-01-01 00:00:00"), null, null, null, null, null, null, null, null, null, null, null)

def transformationDfDim (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumn("daytime_aux", date_format(col("daytime"), "yyyyMMdd"))
    
    dfTrn = dfTrn.withColumn("id_vers", concat(col("daytime_aux"), col("object_id")))

    dfTrn = dfTrn.withColumnRenamed("object_id", "id_eqpm")
                    .withColumnRenamed("name", "des_eqpm")
                    .withColumnRenamed("daytime", "fec_start_date")
                    .withColumnRenamed("end_date", "fec_end_date")
                    .withColumnRenamed("created_date", "fec_create_date")
                    .withColumnRenamed("last_updated_date", "fec_update_date")
                    .withColumnRenamed("eqpm_type", "des_type_eqpm")
                    .withColumnRenamed("pump_model_code", "id_code_pump_model")
                    .withColumnRenamed("pump_ser_number", "num_pump_ser")
                    .withColumnRenamed("tag_id", "id_tag")
                    .withColumnRenamed("uom", "des_uom")
                    .withColumnRenamed("counter_id", "id_counter")
                    .withColumnRenamed("cp_col_point_id", "id_col_point")
                    .withColumnRenamed("op_fcty_1_id", "id_fcty_1")
 
    
    dfTrn = dfTrn.select(order.map(col): _*)

   
    return dfTrn

}

def addRowDummy(df: DataFrame): DataFrame = {
    
    var dfTrn: DataFrame = df

    val dfDummy = spark.createDataFrame(Seq(rowDummy).asJava, df.schema)

    dfTrn = dfTrn.union(dfDummy)

    return dfTrn

}

// COMMAND ----------


var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null
var df_ec_codes: DataFrame  = null

try {
    
        
    df = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_eqpm")).withColumnRenamed("id_bu", "bu")
    df_ec_codes = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_ec_codes")).withColumnRenamed("id_bu", "bu_ec_codes")

    df = df.as("eqpm").join(
                            df_ec_codes.filter(col("code_type") === "EQPM_TYPE").drop("created_date", "last_updated_date").dropDuplicates("code_text", "bu_ec_codes").as("ec_code"), 
                            col("eqpm.eqpm_type") === col("ec_code.code") &&
                            col("eqpm.bu") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("eqpm_type", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "eqpm_type")

    df = df.as("eqpm").join(
                            df_ec_codes.filter(col("code_type") === "EQPM_UOM").drop("created_date", "last_updated_date").dropDuplicates("code_text", "bu_ec_codes").as("ec_code"), 
                            col("eqpm.uom") === col("ec_code.code") &&
                            col("eqpm.bu") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("uom", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "uom")

    df = transformationDfDim(df)
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

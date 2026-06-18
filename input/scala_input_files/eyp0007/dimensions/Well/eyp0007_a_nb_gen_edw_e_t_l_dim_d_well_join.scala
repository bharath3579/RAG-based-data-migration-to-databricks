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

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window

val order = Seq("id_vers", "id_well", "des_well", "id_cds", "des_cds", "des_conduit_name", "id_vers_well_hole", "fec_start_date", "fec_end_date", "bol_alloc_flag", "des_choke_uom", "id_commercial_entity", "id_country_ws", "des_lift_system", "id_field_cds", "des_on_stream_method", "id_fcty_1", "id_well_hookup", "bol_operator_flag", "des_pump_type", "des_well_class", "des_well_function", "id_well_hole", "des_well_main_fluid", "des_well_meter_freq", "des_ref_object", "des_type_well", "fec_create_date", "fec_update_date")
val rowDummy = Row("-99", "-99", null, null, null, null, "-99", java.sql.Timestamp.valueOf("1900-01-01 00:00:00"), null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)

def transformationDfConduit (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("conduit_name", "des_conduit_name")
                 .withColumnRenamed("field_identifier", "id_field_cds")
                 .withColumnRenamed("conduit_number", "join_col_well")
                 .withColumnRenamed("update_date", "dateCDS")
   
    return dfTrn

}

def addRowDummy(df: DataFrame): DataFrame = {
    
    var dfTrn: DataFrame = df

    val dfDummy = spark.createDataFrame(Seq(rowDummy).asJava, df.schema)

    dfTrn = dfTrn.union(dfDummy)

    return dfTrn

}

def joinDfWellAndWellConduit (dfL: DataFrame, dfR: DataFrame): DataFrame = {

    val hWin = Window.partitionBy(col("id_vers"), col("fec_start_date")).orderBy(col("dateCDS").desc)

    // var resDf = dfL.join(dfR, dfL("id_cds") === dfR("join_col_well") && dfR("dateCDS") <= dfL("fec_start_date"), "left")
    var resDf = dfL.join(dfR, dfL("id_cds") === dfR("join_col_well"), "left")

    return resDf.withColumn("new", row_number().over(hWin) ).where(col("new") === 1)

}

def joinDfWellAndWellHole (dfL: DataFrame, dfR: DataFrame): DataFrame = {
    
    val hWin = Window.partitionBy(col("id_well"), col("fec_start_date")).orderBy(col("fec_start_date_well_hole").desc)

    var resDf = dfL.join(dfR, Seq("id_well_hole"), "left").withColumn("id_vers_well_hole", coalesce(dfR("id_vers_well_hole"), lit("-99")))

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
    
    val dfConduit: DataFrame = transformationDfConduit(getDataFromSource(sourcesFinal("aa001163_glb_conduit")))
    val dfWell: DataFrame = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_dim_d_well"))
    val dfWellHole: DataFrame = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hole")).select("id_vers", "id_well_hole", "fec_start_date", "fec_end_date")
                                                                     .withColumnRenamed("id_vers", "id_vers_well_hole")
                                                                     .withColumnRenamed("fec_start_date", "fec_start_date_well_hole")
                                                                     .withColumnRenamed("fec_end_date", "fec_end_date_well_hole")
        
    // dfWellHole(master_sys_code) - left - dfConduit(conduit_number)
    df = joinDfWellAndWellConduit(dfWell, dfConduit)

    df = joinDfWellAndWellHole(df, dfWellHole)

    df = df.select(order.map(col): _*)
    df = addRowDummy(df).withColumn("val_dummy_column_partition", lit(1))

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


val result = "OK"

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)    

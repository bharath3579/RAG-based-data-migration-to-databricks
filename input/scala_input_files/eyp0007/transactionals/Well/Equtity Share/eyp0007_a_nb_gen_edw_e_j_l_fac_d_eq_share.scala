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

import org.apache.spark.sql.functions._
val order = Seq("fec_production_day","id_commercial_entity", "fec_start_date", "fec_end_date", "des_fluid_type", "id_company", "ind_eco_share", "des_partner_role", "ind_field_part", "ind_gross_up", "ind_net_gross_up", "ind_net_part", "bol_tik", "fec_create_date", "fec_update_date", "fec_adhoc_update")


def selectedFields (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df
    
    dfTrn = dfTrn.select(order.map(col): _*)

    return dfTrn
}

def joinWithDim(df1: DataFrame, df2: DataFrame, fieldsPartition: Seq[String], df1Filter1:String, df2Filter1:String, df1Filter2:String, df2Filter2:String, joinType: String): DataFrame = {

    val windowSpec = Window.partitionBy(fieldsPartition.map(df1(_)): _*).orderBy((desc("id_vers")))
    
    val joinedDF = df1.join(df2, df1(df1Filter1) === df2(df2Filter1) && df2(df2Filter2) <= df1(df1Filter2), joinType).withColumn("id_vers", coalesce(df2("id_vers"), lit("-99")))
    val windowDF = joinedDF.withColumn("row_number",row_number().over(windowSpec)).filter("row_number == 1")

    val selectedDF = windowDF.select((df1.columns.map(df1(_)) ++ Seq("id_vers").map(col)):_*)
    selectedDF

}





// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

val actualDate = java.time.LocalDate.now

var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null


val dfFacEqShare = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_fac_d_eq_share")).filter(col("fec_start_date") <= col("fec_end_date") ||  col("fec_end_date").isNull)
val dfDimComEntity =  getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_com_entity"))
val dfDimCompany = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_company"))


val dfFacEqShareDaily =  dfFacEqShare.withColumn("fec_end_date_aux", 
                                                    when(col("fec_end_date").isNull && col("fec_start_date") > actualDate, col("fec_start_date"))
                                                    .when(col("fec_end_date").isNull && col("fec_start_date") <= actualDate, actualDate)
                                                    .otherwise(date_sub(col("fec_end_date"), 1)))
                                     .withColumn("fec_end_date_aux_formatted", col("fec_end_date_aux").cast(DateType))
                                     .withColumn("fec_start_date_formatted", col("fec_start_date").cast(DateType))
                                     .withColumn("date_range",
                                            expr("sequence(fec_start_date_formatted, fec_end_date_aux_formatted, interval 1 day)"))
                                     .withColumn("fec_production_day",
                                            explode(col("date_range")))
                                     .withColumn("fec_production_day",
                                            to_timestamp(col("fec_production_day")))
                                     .withColumn("year", date_format(to_date(col("fec_production_day")), "yyyy"))
                                     .filter(col("year") > "2009")
                                     .withColumn("fec_adhoc_update", col("fec_production_day"))

                                    
                                     



try {
    
    df = joinWithDim(dfFacEqShareDaily, dfDimComEntity, Seq("fec_production_day", "object_id", "ref_object_id", "des_fluid_type"), "object_id", "id_commercial_entity", "fec_production_day", "fec_start_date", "left")
    df = df.withColumnRenamed("id_vers", "id_commercial_entity")

    df = joinWithDim(df, dfDimCompany, Seq("fec_production_day", "object_id", "ref_object_id", "des_fluid_type"), "ref_object_id", "id_company", "fec_production_day", "fec_start_date", "left")
    df = df.withColumnRenamed("id_vers", "id_company")

    df = selectedFields(df)
    val dfFormatted = applyDDLFromTableToDF(df,getObject(targetFinal("target_object").toString))
    writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

    dlMsg = "Rows Inserted: " + df.count
    dlInsert = "OK"

} catch {
   case e: Throwable => 
                error = e.toString
                dlInsert = "KO"
                LogHelper().logEndKO(dlMsg)
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

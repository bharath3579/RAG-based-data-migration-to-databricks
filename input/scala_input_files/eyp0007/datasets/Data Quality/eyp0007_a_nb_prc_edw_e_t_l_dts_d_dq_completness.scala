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

def readDfFromTable(tableName: String) : DataFrame = {
    val full_table_name = getObject(s"datahub01%env%aucdatagold.eyp_operational.${tableName}")
    val sourceMap = Map(
        "source_object" -> s"${full_table_name}",
        "source_format" -> "table",
        "source_alias" -> s"${tableName}"
    )
    var df = getDataFromSource(sourceMap)
    return df
}

val selectFieldsSeq = Seq("fec_process","fec_start_date","fec_end_date" ,"id_ec_object","des_object_name","id_well","id_commercial_entity","id_facility_class_1","des_facility_class_1","id_well_hookup",
"id_well_hole","id_eqpm","id_wellbore","id_area","des_area","id_productionunit","des_productunit","id_geo_area","id_field_cds","id_field","id_basin","id_operator_route","id_col_point",
"id_licence", "SOURCE_DATASET_NAME", "BUSINESS_TERM", "ind_warning")

val selectFieldsDimSeq : Seq[String] = selectFieldsSeq.filter(x => (x.startsWith("id_")))

def selectFields (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    val selectFields = selectFieldsSeq 

    dfTrn = dfTrn.select(selectFields.map(col): _*)

    dfTrn = dfTrn.withColumnRenamed("SOURCE_DATASET_NAME", "des_functional_dataset")
                 .withColumnRenamed("BUSINESS_TERM", "des_attribute")
    dfTrn = dfTrn.na.fill("-99", selectFieldsDimSeq)
   
    return dfTrn

}


// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

val dqFile = getDataFromSource(sourcesFinal("am_eyp0007_glb_attribute_for_dq")).filter(col("rule_type") === "COMPLETNESS")
val rangeTables: Seq[String] = dqFile.select("DATADRIVEN_DATASET_NAME").distinct.map(_.getString(0)).collect.toSeq

var dlInsert = "N/A"
var dlMsg = ""
var df: DataFrame  = null

try {
        rangeTables.foreach( x => {
                    val tableName: String = x
                    val full_table_name = getObject(s"datahub01%env%aucdatagold.eyp_operational.${if (tableName == "eyp0007_tb_fac_d_well") s"am_$tableName" else tableName}")
                    val sourceMap = Map(
                        "source_object" -> s"${full_table_name}",
                        "source_format" -> "table",
                        "source_alias" -> s"${tableName}"
                    )
                    var dfTrn: DataFrame =getDataFromSource(sourceMap).withColumn("table_name", lit(x))
                    
                    if (df == null) {
                    df = dfTrn
                    }else {
                    df = df.unionByName(dfTrn, allowMissingColumns=true)
                        }
                    }) 
                    
                    // join con el fichero de data quality
                    val dfJoin = dqFile.join(df, dqFile("DATADRIVEN_DATASET_NAME") === df("table_name") && dqFile("DATADRIVEN_ATTRIBUTE_NAME") === df("attribute"), "inner")
                   
                   
                    // añadimos la columna de comprobacion
                    val finalDf = dfJoin.withColumn("ind_warning",
                                            when(col("value").isNotNull, lit(0)).    // 0 -> no nulo
                                            otherwise(lit(1))                        // 1 -> nulo
                                    )
                                    .filter(col("ind_warning") === 1)
                                    .withColumn("fec_process", to_timestamp(date_format(current_timestamp(), "yyyy-MM-dd")))
                    
                    val finalDfselect = selectFields(finalDf)
                                .withColumn("year",  date_format(to_date(col("fec_process")), "yyyy").cast(IntegerType))
                                .withColumn("month", date_format(to_date(col("fec_process")), "MM").cast(IntegerType))
                                .withColumn("day",   date_format(to_date(col("fec_process")), "dd").cast(IntegerType))
                    
                    val dfFormatted = applyDDLFromTableToDF(finalDfselect,getObject(targetFinal("target_object").toString))
                    writeWrapper(sourceDF=dfFormatted,conf=targetFinal,prefix="target",operationName = target_operation_name,optimize=optimizeDelta)
                    
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

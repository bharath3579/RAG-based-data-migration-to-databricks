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
 val error =""
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

val orderFields: Seq[String] = Seq("cod_resp_org", "cod_long_id", "des_kind", "des_address_1", "des_address_2", "des_address_3", "des_postal_code", "des_postal_country", "des_more_address", 
"des_description", "des_data_source", "fec_create", "fec_create_user_id", "fec_update", "fec_update_user_id", "cod_org_long", "des_org_kind", "fec_start", "fec_end", "cod_installation")

def transformRespOrg(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("identifier", "cod_resp_org")
    .withColumnRenamed("long_identifier", "cod_long_id")
    .withColumnRenamed("start_date", "fec_start").withColumn("fec_start", col("fec_start").cast("timestamp"))
    .withColumnRenamed("end_date", "fec_end")
    .withColumnRenamed("kind", "des_kind")
    .withColumnRenamed("address_1", "des_address_1")
    .withColumnRenamed("address_2", "des_address_2")
    .withColumnRenamed("address_3", "des_address_3")
    .withColumnRenamed("postal_code", "des_postal_code")
    .withColumnRenamed("postal_country", "des_postal_country")
    .withColumnRenamed("more_address", "des_more_address")
    .withColumnRenamed("description", "des_description")
    .withColumnRenamed("data_source", "des_data_source")
    .withColumnRenamed("create_date", "fec_create").withColumn("fec_create", col("fec_create").cast("timestamp"))
    .withColumnRenamed("create_user_id", "fec_create_user_id").withColumn("fec_create_user_id", col("fec_create_user_id").cast("timestamp"))
    .withColumnRenamed("update_date", "fec_update").withColumn("fec_update", col("fec_update").cast("timestamp"))
    .withColumnRenamed("update_user_id", "fec_update_user_id").withColumn("fec_update_user_id", col("fec_update_user_id").cast("timestamp"))

  dfTrn
}


def transformFieldRespOrg(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("field_identifier", "cod_field")
    .withColumnRenamed("field_name", "des_field")
    .withColumnRenamed("country_name", "des_country")
    .withColumnRenamed("field_type", "des_field_type")
    .withColumnRenamed("organization_identifier", "cod_resp_org")
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
    .drop("fec_start")
    .drop("fec_end")

  dfTrn
}


def transformInstallRespOrg(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("installation_identifier", "cod_installation")
    .withColumnRenamed("installation_name", "des_installation")
    .withColumnRenamed("installation_type", "des_installation_type")
    .withColumnRenamed("organization_identifier", "cod_resp_org")
    .withColumnRenamed("organization_long_id", "des_field")
    .withColumnRenamed("organization_kind", "des_country")
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

def joinRespOrg(df1: DataFrame, df2: DataFrame, joinType: String = "inner"): DataFrame = {
  val dfJoined = df1.join(
    df2,
    df1("cod_resp_org") === df2("cod_resp_org"),
    joinType
  )
  val dfClean = dfJoined.drop(df2("cod_resp_org"))
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


var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {   

        val dfRespOrg = transformRespOrg(getDataFromSource(sourcesFinal("aa001163_glb_cds_responsible_organization")))

        val dfFieldRespOrg = transformFieldRespOrg(getDataFromSource(sourcesFinal("aa001163_glb_cds_field_resp_org")))

        val dfInstRespOrg = transformInstallRespOrg(getDataFromSource(sourcesFinal("aa001163_glb_cds_installation_resp_org")))

        val df = joinRespOrg(dfRespOrg, dfFieldRespOrg, "left")

        val df2 = joinRespOrg(df, dfInstRespOrg, "left")

        val dfFinal = df2.select(orderFields.map(col): _*)
        dfFinal.printSchema

        //val dfFinal= df2Final.dropDuplicates("cod_resp_org")
        dfFinal.printSchema

        val count= dfFinal.count
        println("Numero de rows:")
        println(count)

        // writeDataFrame(dfFinal, targetFinal)
        val dfFormatted = applyDDLFromTableToDF(dfFinal,getObject((target_object).toString))
        writeWrapper(sourceDF=dfFormatted,conf=targetFinal,prefix="target",operationName = target_operation_name,optimize=optimizeDelta)

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

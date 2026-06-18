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

val orderFields: Seq[String] = Seq("cod_installation","des_installation","des_country","des_existence_kind","des_type","des_function",
"des_status","fec_installation","fec_removal","des_operator","des_onshore_offshore","val_latitude","val_longitude","des_geo_coord_sys",
"val_x_coord","val_y_coord","des_xy_unit","des_xy_coord_sys","val_z_coord","des_vertical_unit","des_vertical_datum",
"des_location_type","des_description","des_data_source",
"fec_create","cod_create_user","fec_update","cod_update_user", "cod_field","des_field",
"des_responsible_org","des_org_long_id","des_facility_short_cod",
"des_facility_tag_short_cod","des_sap_functional_cod")


def addAliasColumns(df: DataFrame): DataFrame = {
  df
    .withColumn(
      "des_facility_short_cod",
      when(col("des_alias_type") === "Facility Short Code", col("des_alias")).otherwise(null).cast("string")
    )
    .withColumn(
      "des_facility_tag_short_cod",
      when(col("des_alias_type") === "Facility Tag Short Code", col("des_alias")).otherwise(null).cast("string")
    )
    .withColumn(
      "des_sap_functional_cod",
      when(col("des_alias_type") === "SAP Functional Location", col("des_alias")).otherwise(null).cast("string")
    )
}
def transformInstallation(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("identifier", "cod_installation")
    .withColumnRenamed("name", "des_installation")
    .withColumnRenamed("country_name", "des_country")
    .withColumnRenamed("type", "des_type")
    .withColumnRenamed("onshore_or_offshore", "des_onshore_offshore")
    .withColumnRenamed("location_type", "des_location_type")
    .withColumnRenamed("existence_kind", "des_existence_kind")
    .withColumnRenamed("status", "des_status")
    .withColumnRenamed("operator", "des_operator")
    .withColumnRenamed("installation_date", "fec_installation").withColumn("fec_installation", col("fec_installation").cast("timestamp"))
    .withColumnRenamed("removal_date", "fec_removal").withColumn("fec_removal", col("fec_removal").cast("timestamp"))
    .withColumnRenamed("latitude", "val_latitude").withColumn("val_latitude", col("val_latitude").cast("double"))
    .withColumnRenamed("longitude", "val_longitude").withColumn("val_longitude", col("val_longitude").cast("double"))
    .withColumnRenamed("function", "des_function")
    .withColumnRenamed("geographic_coordinate_system", "des_geo_coord_sys")
    .withColumnRenamed("x_coordinate", "val_x_coord").withColumn("val_x_coord", col("val_x_coord").cast("double"))
    .withColumnRenamed("y_coordinate", "val_y_coord").withColumn("val_y_coord", col("val_y_coord").cast("double"))
    .withColumnRenamed("z_coordinate", "val_z_coord").withColumn("val_z_coord", col("val_z_coord").cast("double"))
    .withColumnRenamed("xy_coordinate_system", "des_xy_coord_sys")
    .withColumnRenamed("xy_unit", "des_xy_unit")
    .withColumnRenamed("y_coordinate", "val_y_coord").withColumn("val_y_coord", col("val_y_coord").cast("double"))
    .withColumnRenamed("y_coordinate", "val_y_coord").withColumn("val_y_coord", col("val_y_coord").cast("double"))
    .withColumnRenamed("y_coordinate", "val_y_coord").withColumn("val_y_coord", col("val_y_coord").cast("double"))
    .withColumnRenamed("y_coordinate", "val_y_coord").withColumn("val_y_coord", col("val_y_coord").cast("double"))
    .withColumnRenamed("vertical_unit", "des_vertical_unit")
    .withColumnRenamed("vertical_datum", "des_vertical_datum")
    .withColumnRenamed("description", "des_description")
    .withColumnRenamed("data_source", "des_data_source")
    .withColumnRenamed("create_date", "fec_create").withColumn("fec_create", col("fec_create").cast("timestamp"))
    .withColumnRenamed("create_user_id", "cod_create_user")
    .withColumnRenamed("update_date", "fec_update").withColumn("fec_update", col("fec_update").cast("timestamp"))
    .withColumnRenamed("update_user_id", "cod_update_user")

  dfTrn
}


def transformInstAlias(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("installation_identifier", "cod_installation")
    .withColumnRenamed("installation_name", "des_installation")
    .withColumnRenamed("installation_type", "des_installation_type")
    .withColumnRenamed("alias", "des_alias")
    .withColumnRenamed("alias_type", "des_alias_type")
    .withColumnRenamed("start_date", "fec_start").withColumn("fec_start", col("fec_start").cast("timestamp"))
    .withColumnRenamed("end_date", "fec_end").withColumn("fec_end", col("fec_end").cast("timestamp"))
    .withColumnRenamed("create_date", "fec_create").withColumn("fec_create", col("fec_create").cast("timestamp"))
    .withColumnRenamed("create_user_id", "cod_create_user")
    .withColumnRenamed("update_date", "fec_update").withColumn("fec_update", col("fec_update").cast("timestamp"))
    .withColumnRenamed("update_user_id", "cod_update_user")
    .drop("description")
    .drop("data_source")
    .drop("des_installation")
    .drop("des_installation_type")
    .drop("fec_start")
    .drop("fec_end")
    .drop("fec_create")
    .drop("cod_create_user")
    .drop("fec_update")
    .drop("cod_update_user")

  dfTrn
}


def transformInstallWellConn(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("installation_identifier", "cod_installation")
    .withColumnRenamed("installation_name", "des_installation")
    .withColumnRenamed("installation_type", "des_installation_type")
    .withColumnRenamed("unique_well_identifier", "cod_well")
    .withColumnRenamed("well_name", "des_well")
    .withColumnRenamed("start_date", "fec_start").withColumn("fec_start", col("fec_start").cast("timestamp"))
    .withColumnRenamed("end_date", "fec_end").withColumn("fec_end", col("fec_end").cast("timestamp"))
    .withColumnRenamed("create_date", "fec_create").withColumn("fec_create", col("fec_create").cast("timestamp"))
    .withColumnRenamed("create_user_id", "cod_create_user")
    .withColumnRenamed("update_date", "fec_update").withColumn("fec_update", col("fec_update").cast("timestamp"))
    .withColumnRenamed("update_user_id", "cod_update_user")
    .drop("description")
    .drop("data_source")
    .drop("des_installation")
    .drop("des_installation_type")
    .drop("fec_start")
    .drop("fec_end")
    .drop("fec_create")
    .drop("cod_create_user")
    .drop("fec_update")
    .drop("cod_update_user")

  dfTrn
}


def transformInstallFieldAssoc(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("installation_identifier", "cod_installation")
    .withColumnRenamed("field_identifier", "cod_field")
    .withColumnRenamed("field_name", "des_field")
    .withColumnRenamed("start_date", "fec_start").withColumn("fec_start", col("fec_start").cast("timestamp"))
    .withColumnRenamed("end_date", "fec_end").withColumn("fec_end", col("fec_end").cast("timestamp"))
    .withColumnRenamed("create_date", "fec_create").withColumn("fec_create", col("fec_create").cast("timestamp"))
    .withColumnRenamed("create_user_id", "cod_create_user")
    .withColumnRenamed("update_date", "fec_update").withColumn("fec_update", col("fec_update").cast("timestamp"))
    .withColumnRenamed("update_user_id", "cod_update_user")
    .drop("description")
    .drop("data_source")
    .drop("fec_start")
    .drop("fec_end")
    .drop("fec_create")
    .drop("cod_create_user")
    .drop("fec_update")
    .drop("cod_update_user")
    .drop("des_country")

  dfTrn
}


def transformRespOrg(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn
    .withColumnRenamed("installation_identifier", "cod_installation")
    .withColumnRenamed("organization_identifier", "des_responsible_org")
    .withColumnRenamed("organization_long_id", "des_org_long_id")
    .drop("description")
    .drop("data_source")
    .drop("des_installation_name")
    .drop("des_installation_type")
    .drop("start_date")
    .drop("end_date")
    .drop("create_date")
    .drop("create_user_id")
    .drop("update_date")
    .drop("update_user_id")

  dfTrn
}

def joinInstallation(df1: DataFrame, df2: DataFrame, joinType: String = "inner"): DataFrame = {
  val dfJoined = df1.join(
    df2,
    df1("cod_installation") === df2("cod_installation"),
    joinType
  )
  val dfClean = dfJoined
    .drop(df2("cod_installation"))
  dfClean
}

// COMMAND ----------

def addAliasColumns(df: DataFrame): DataFrame = {
  df
    .withColumn(
      "des_facility_short_cod",
      when(col("des_alias_type") === "Facility Short Code", col("des_alias")).otherwise(lit("")).cast("string")
    )
    .withColumn(
      "des_facility_tag_short_cod",
      when(col("des_alias_type") === "Facility Tag Short Code", col("des_alias")).otherwise(lit("")).cast("string")
    )
    .withColumn(
      "des_sap_functional_cod",
      when(col("des_alias_type") === "SAP Functional Location", col("des_alias")).otherwise(lit("")).cast("string")
    )
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

        val dfInstall = transformInstallation(getDataFromSource(sourcesFinal("aa001163_glb_cds_installation")))

        val dfInstallClass = getDataFromSource(sourcesFinal("aa001163_glb_cds_installation_class"))

        val dfInstallAlias = transformInstAlias(getDataFromSource(sourcesFinal("aa001163_glb_cds_installation_alias")))

        val dfInstallWellConn = transformInstallWellConn(getDataFromSource(sourcesFinal("aa001163_glb_cds_inst_well_connection")))

        val dfInstallFieldAssoc = transformInstallFieldAssoc(getDataFromSource(sourcesFinal("aa001163_glb_cds_inst_field_assoc")))

        val dfInstallRespOrg = transformRespOrg(getDataFromSource(sourcesFinal("aa001163_glb_cds_installation_resp_org")))

        val df = joinInstallation(dfInstall, dfInstallAlias, "left")

        val df2 = joinInstallation(df, dfInstallWellConn, "left")

        val df3 = joinInstallation(df2, dfInstallFieldAssoc, "left")

        val df4 = joinInstallation(df3, dfInstallRespOrg, "left")

        val df5 = addAliasColumns(df4)

        val df6 = df5.dropDuplicates()

        val dfFinal = df6.select(orderFields.map(col): _*)

        // writeDataFrame(dfFinal, targetFinal)
        val dfFormatted = applyDDLFromTableToDF(dfFinal,getObject((target_object).toString))
        writeWrapper(sourceDF=dfFormatted,conf=targetFinal,prefix="target",operationName = target_operation_name,optimize=optimizeDelta)
        //dfFinal.printSchema

        val count= dfFinal.count
        println("Numero de rows:")
        println(count)

        //display(dfFinal)

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

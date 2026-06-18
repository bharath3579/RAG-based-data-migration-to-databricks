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

def toTranspose(df: DataFrame, columnsFixed: Seq[String], columnsTransposed: Seq[String]): DataFrame = {
  val transposeDf = df.select(
    (columnsFixed.map(col) ++
    Seq(expr(s"stack(${columnsTransposed.length}," + 
      columnsTransposed.map(colName => s"'$colName', $colName").mkString(", ") + ") as (attribute, value)"))): _*
  ).select((columnsFixed.map(col) ++ Seq(col("attribute"), col("value"))): _*)
  transposeDf
}

// COMMAND ----------

var dlInsert = "N/A"
var dlMsg = ""

val perf_int = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_perf_interv"))
  .filter("id_well_bore_int is not null")
  .selectExpr("id_vers as id_vers_perf_int", "id_perf_interval", "des_perf_interval", "id_resv_block_formation", "id_well_bore_int", "id_well_bore")

val res_bl_for = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_res_block_for"))
  .selectExpr("id_vers as id_vers_res_bl_for", "id_resv_block_form", "cod_resv_block_form")

val wbore_int = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_wellbore_int"))
  .selectExpr("id_vers as id_vers_wbore_int", "id_well_bore_interval", "des_well_bore_interval", "id_well_bore")

val wbore = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_dim_d_well_bore"))
  .selectExpr("id_vers as id_vers_wbore", "id_well_bore", "des_well_bore", "des_country", "id_well")

val well = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_dim_d_well"))
  .selectExpr("id_vers as id_vers_well", "id_well", "des_well")

try {

  val dfFinal = well.alias("well")
              .join(wbore.alias("wbore"), "id_well")
              .join(wbore_int.alias("wbore_int"), "id_well_bore")
              .join(perf_int.alias("perf_int"), col("id_well_bore_int") === col("id_well_bore_interval"))
              .join(res_bl_for.alias("res_bl_for"), col("id_resv_block_form") === col("id_resv_block_formation"))

  val dfWrite = dfFinal.selectExpr(
  "concat(substring(well.id_vers_well, 1, 8), wbore.id_vers_wbore) as id_final",
  "well.id_vers_well", 
  "concat(substring(well.id_vers_well, 1, 8), wbore.id_vers_wbore) as `id_vers_wbore`", 
  "wbore_int.id_vers_wbore_int", 
  "perf_int.id_vers_perf_int", 
  "res_bl_for.id_vers_res_bl_for",
  "des_well", 
  "des_well_bore", 
  "des_country", 
  "des_well_bore_interval", 
  "des_perf_interval", 
  "cod_resv_block_form"
  ).na.fill(
    dlInsert, 
    Seq("des_well", "des_well_bore", "des_country", "des_well_bore_interval", "des_perf_interval", "cod_resv_block_form")
  )
  val dfFormatted = applyDDLFromTableToDF(dfWrite,getObject(targetFinal("target_object").toString))
  writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

  dlMsg = "Rows Inserted: " + dfWrite.count 
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


val result = s"""{"error": "$error","dlInsert": { "result": "$dlInsert", "msg": "$dlMsg" }}"""

if (uniqueKey.nonEmpty && step > 0)
    ControlHelper().updateControlTable(spark, uniqueKey, step, s"$name: $result")

if (!error.isEmpty || dlInsert.equals("KO")) {
    LogHelper().logError(s"$name: $result")
    throw new Exception(s"$name: $result")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

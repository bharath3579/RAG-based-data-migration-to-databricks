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
import org.apache.spark.sql.functions._


val selectFieldsSeq = Seq("fec_production_day","id_vers_well","id_well","id_commercial_entity","id_facility_class_1","id_well_hookup",
"id_well_hole","id_area","id_productionunit","id_geo_area","id_field_cds","id_field","id_basin","id_operator_route","id_col_point",
"id_licence","des_choke_prod","ind_avg_choke_size","ind_avg_choke_size_2","des_choke_uom","ind_avg_gl_choke_size","des_avg_gl_choke_uom",
"des_comments","ind_calc_on_stream_hrs","ind_annulus_press_psig","ind_annulus_press_barg","ind_avg_bh_press_psig","ind_avg_bh_press_barg",
"ind_avg_bh_temp_f","ind_avg_bh_temp_c","ind_avg_gl_diff_press_psig","ind_avg_gl_diff_press_barg","ind_avg_gl_press_psig",
"ind_avg_gl_press_barg","ind_avg_gl_rate_mscf","ind_avg_gl_rate_kscf","ind_avg_gl_rate_sm3","ind_avg_gl_rate_mscfperday",
"ind_avg_gl_rate_kscfperday","ind_avg_gl_rate_sm3perday","ind_avg_wh_dsc_press_psig","ind_avg_wh_dsc_press_barg","ind_avg_wh_dsc_temp_f",
"ind_avg_wh_dsc_temp_c","ind_avg_wh_press_psig","ind_avg_wh_press_barg","ind_avg_wh_press_2_psig","ind_avg_wh_press_2_barg",
"ind_avg_wh_temp_f","ind_avg_wh_temp_c","ind_avg_wh_temp_2_f","ind_avg_wh_temp_2_c","fec_create_date","fec_update_date"
)

def selectFields (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    val selectFields = selectFieldsSeq 

    dfTrn = dfTrn.select(selectFields.map(col): _*)
   
    return dfTrn

}

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------


var dlInsert = "N/A"
var dlMsg = ""

val flagFullLoad = paramsFinal.getOrElse("full_load", "false").toString.toBoolean

var df: DataFrame  = null

var dfTransactional: DataFrame  = null

if (!flagFullLoad){
            val fieldsFilter = auxParamsFinal.getOrElse("fieldsFilter", List("fec_create_date", "fec_update_date")).asInstanceOf[List[String]]

            var referenceDateAux = if (referenceDate!= null) {referenceDate} else {""}
            if (referenceDateAux.isEmpty){
                referenceDateAux = returnDateNowString()
            }

            val filterWhere = getWhereFilterDays((referenceDateAux), fieldsFilter, 500)
            dfTransactional = getDataFromSource(sourcesFinal.apply(0), filterWhere)
}else {
            dfTransactional = getDataFromSource(sourcesFinal.apply(0))
        }


try {

    df = selectFields(dfTransactional)

    df = df.withColumn("__calculated_key__", concat_ws("_", auxParamsFinal.get("keys").map(toScalaList).getOrElse(Seq.empty[String]).map(col): _*))

    val (targetDf, targetDelta) = getDeltaTable(df, targetFinal)
    applyChange(targetFinal, df)
 
    optimizeDeltaFun(targetDelta, getObject(targetFinal("target_object").toString) ,auxParamsFinal("dayOfWeek").toString)

    dlMsg = "Rows Inserted: " + df.count
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

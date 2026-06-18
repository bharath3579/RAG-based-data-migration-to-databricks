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

val selectFieldsSeq = Seq("fec_production_day","id_vers_well","id_well","id_commercial_entity","id_facility_class_1","id_well_hookup",
  "id_well_hole","id_area","id_productionunit","id_geo_area","id_field_cds","id_field","id_basin","id_operator_route","id_col_point",
  "id_licence","ind_z_o_alloc_gl_vol_kscf","ind_alloc_cond_vol_sm3", "ind_alloc_cond_vol_stb", "ind_alloc_gas_vol_kscf", "ind_alloc_gas_vol_mscf", "ind_alloc_gas_vol_sm3", 
  "ind_alloc_gl_vol_kscf", "ind_alloc_gl_vol_mscf", "ind_alloc_gl_vol_sm3", "ind_alloc_net_oil_vol_sm3", "ind_alloc_net_oil_vol_stb", 
  "ind_alloc_water_vol_bbl", "ind_alloc_water_vol_m3", "ind_alloc_water_vol_sm3", "ind_alloc_water_vol_stb", "ind_o_alloc_cond_vol_15_sm3", 
  "ind_o_alloc_cond_vol_15_stb", "ind_o_alloc_cond_vol_sm3", "ind_o_alloc_cond_vol_stb", "ind_o_alloc_gas_vol_15_kscf", "ind_o_alloc_gas_vol_15_mscf", 
  "ind_o_alloc_gas_vol_15_sm3", "ind_o_alloc_gas_vol_20_kscf", "ind_o_alloc_gas_vol_20_mscf", "ind_o_alloc_gas_vol_20_sm3", "ind_o_alloc_gas_vol_kscf", 
  "ind_o_alloc_gas_vol_mscf", "ind_o_alloc_gas_vol_sm3", "ind_o_alloc_net_oil_vol_15_sm3", "ind_o_alloc_net_oil_vol_15_stb", 
  "ind_o_alloc_net_oil_vol_20_sm3", "ind_o_alloc_net_oil_vol_20_stb", "ind_o_alloc_net_oil_vol_sm3", "ind_o_alloc_net_oil_vol_stb", 
  "ind_o_alloc_water_vol_bbl", "ind_o_alloc_water_vol_m3", "ind_p_alloc_cond_vol_sm3", "ind_p_alloc_cond_vol_stb", "ind_p_alloc_gas_vol_15_kscf", 
  "ind_p_alloc_gas_vol_15_mscf", "ind_p_alloc_gas_vol_15_sm3", "ind_p_alloc_gas_vol_kscf", "ind_p_alloc_gas_vol_mscf", "ind_p_alloc_gas_vol_sm3", 
  "ind_p_alloc_net_oil_vol_15_sm3", "ind_p_alloc_net_oil_vol_15_stb", "ind_p_alloc_net_oil_vol_20_sm3", "ind_p_alloc_net_oil_vol_20_stb", 
  "ind_p_alloc_net_oil_vol_sm3", "ind_p_alloc_net_oil_vol_stb", "ind_p_alloc_water_vol_bbl", "ind_p_alloc_water_vol_m3", "ind_theor_cond_rate_sm3", 
  "ind_theor_cond_rate_sm3perday", "ind_theor_cond_rate_stb", "ind_theor_cond_rate_stbperday", "ind_theor_gas_rate_kscf", "ind_theor_gas_rate_kscfperday", 
  "ind_theor_gas_rate_mscf", "ind_theor_gas_rate_mscfperday", "ind_theor_gas_rate_sm3", "ind_theor_gas_rate_sm3perday", "ind_theor_gl_rate_kscf", 
  "ind_theor_gl_rate_kscfperday", "ind_theor_gl_rate_mscf", "ind_theor_gl_rate_mscfperday", "ind_theor_gl_rate_sm3", "ind_theor_gl_rate_sm3perday", 
  "ind_theor_net_oil_rate_sm3", "ind_theor_net_oil_rate_sm3perday", "ind_theor_net_oil_rate_stb", "ind_theor_net_oil_rate_stbperday", 
  "ind_theor_water_rate_bbl", "ind_theor_water_rate_m3", "ind_theor_water_rate_sm3perday", "ind_theor_water_rate_stbperday", "fec_create_date", "fec_update_date"
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
            dfTransactional = getDataFromSource(sourcesFinal("am_eyp0007_tb_fac_d_pwel_day_all_full"), filterWhere)
}else {
            dfTransactional = getDataFromSource(sourcesFinal("am_eyp0007_tb_fac_d_pwel_day_all_full"))
        }



try {

    df = selectFields(dfTransactional)
    
    df = df.withColumn("__calculated_key__", concat_ws("_", auxParamsFinal.get("keys").map(toScalaList).getOrElse(Seq.empty[String]).map(col): _*))

     if (flagFullLoad) {
        val dfFormatted = applyDDLFromTableToDF(df,getObject(targetFinal("target_object").toString))
        writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)
    }else {
      val (targetDf, targetDelta) = getDeltaTable(df, targetFinal)
      applyChange(targetFinal, df)
  
      optimizeDeltaFun(targetDelta, getObject(targetFinal("target_object").toString) ,auxParamsFinal("dayOfWeek").toString)
    }
    
    println("Rows Inserted: " + df.count)
   

}  catch {
    case e: Throwable => 
                error = e.toString
                println(error)
                LogHelper().logEndKO(error)
}

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

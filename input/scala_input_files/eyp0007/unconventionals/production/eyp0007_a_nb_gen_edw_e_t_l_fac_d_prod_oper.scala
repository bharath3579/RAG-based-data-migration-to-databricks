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

import org.apache.spark.sql.functions._
val selectFieldsSeq = Seq("__calculated_key__","fec_production_day","id_vers_well","id_well","id_well_hole","ind_alloc_cond_vol_stb", 
                          "ind_alloc_gas_vol_mscf", "ind_alloc_oil_vol_stb", "ind_alloc_water_vol_bbl","ind_avg_bh_press_psig", "ind_avg_choke_size",
                          "ind_annulus_press_psig", "ind_avg_wh_temp_2_f", "ind_avg_wh_press_psig", "ind_avg_wh_dsc_press_psig", "ind_avg_wh_temp_f", "ind_avg_wh_dsc_temp_f"
                          , "fec_create_date", "fec_update_date")

def selectFields (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    val selectFields = selectFieldsSeq 

    dfTrn = dfTrn.select(selectFields.map(col): _*)
   
    return dfTrn

}

def transformOper (): DataFrame = {

    var dfTransactional: DataFrame  = null

    val flagFullLoad = paramsFinal.getOrElse("full_load", "false").toString.toBoolean

    if (!flagFullLoad) {

        val fieldsFilter = auxParamsFinal.getOrElse("fieldsFilter", List("fec_create_date", "fec_update_date")).asInstanceOf[List[String]]

            var referenceDateAux = if (referenceDate!= null) {referenceDate} else {""}
            if (referenceDateAux.isEmpty){
                referenceDateAux = returnDateNowString()
            }
            val filterWhere = getWhereFilterDays((referenceDateAux), fieldsFilter, 500)
            dfTransactional = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_pwl_stat_oper"), filterWhere).filter(substring(col("id_productionunit"), 9, 1000).isin("68779B681C404188B7CF8903F88DBB51", "C7F0D5F83C6D4FBFB43693248F5F8270"))
    }else {
            dfTransactional = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_pwl_stat_oper")).filter(substring(col("id_productionunit"), 9, 1000).isin("68779B681C404188B7CF8903F88DBB51", "C7F0D5F83C6D4FBFB43693248F5F8270"))
        }

    dfTransactional = dfTransactional.select("__calculated_key__","fec_production_day","id_vers_well","id_well","id_well_hole","ind_avg_bh_press_psig", "ind_avg_choke_size",
    "ind_annulus_press_psig", "ind_avg_wh_temp_2_f", "ind_avg_wh_press_psig", "ind_avg_wh_dsc_press_psig", "ind_avg_wh_temp_f", "ind_avg_wh_dsc_temp_f", "fec_create_date", "fec_update_date")

return dfTransactional

    
}


def transformStat (): DataFrame = {

    var dfTransactional: DataFrame  = null

    val flagFullLoad = paramsFinal.getOrElse("full_load", "false").toString.toBoolean

    if (!flagFullLoad) {

        val fieldsFilter = auxParamsFinal.getOrElse("fieldsFilter", List("fec_create_date", "fec_update_date")).asInstanceOf[List[String]]

            var referenceDateAux = if (referenceDate!= null) {referenceDate} else {""}
            if (referenceDateAux.isEmpty){
                referenceDateAux = returnDateNowString()
            }
            val filterWhere = getWhereFilterDays((referenceDateAux), fieldsFilter, 500)
            dfTransactional = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_pwl_stat_prod"), filterWhere).filter(substring(col("id_productionunit"), 9, 1000).isin("68779B681C404188B7CF8903F88DBB51", "C7F0D5F83C6D4FBFB43693248F5F8270"))
    }else {
            dfTransactional = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_pwl_stat_prod")).filter(substring(col("id_productionunit"), 9, 1000).isin("68779B681C404188B7CF8903F88DBB51", "C7F0D5F83C6D4FBFB43693248F5F8270"))
        }

    dfTransactional = dfTransactional.select("__calculated_key__","ind_alloc_cond_vol_stb", "ind_alloc_gas_vol_mscf", "ind_alloc_net_oil_vol_stb", "ind_alloc_water_vol_bbl")
                                     .withColumnRenamed("ind_alloc_net_oil_vol_stb", "ind_alloc_oil_vol_stb")

return dfTransactional

    
}


def joinOperProd(dfL: DataFrame, dfR: DataFrame): DataFrame = {
  val left  = dfL.dropDuplicates(Seq("__calculated_key__"))
  val right = dfR.dropDuplicates(Seq("__calculated_key__"))
  left.join(right, Seq("__calculated_key__"), "full_outer")
}

// Leemos Well hole filtrando por los estados de EFD2 y MBU. 
def transformWellHole (df: DataFrame): DataFrame = {
    
    var dfTrn: DataFrame = df

    dfTrn = dfTrn.filter(col("des_state_or_province").isin("PENNSYLVANIA", "TEXAS", "NEW YORK"))
                     .select("id_well_hole", "id_vers").orderBy(desc("id_vers"))
                     .dropDuplicates("id_well_hole")
                    

    return dfTrn
}

// Hacemos join con Well hole de Unconventionals para modificar la FK, pues la que trae el transaccional es de Well hole de EC
def joinWellHole (dfL: DataFrame, dfR: DataFrame): DataFrame = {

    var dfTrn: DataFrame = null
    dfTrn = dfL.withColumn("id_well_hole", substring(col("id_well_hole"), 9, 1000))
    dfTrn = dfTrn.join(dfR, Seq("id_well_hole"), "inner")
    dfTrn = dfTrn.drop("id_well_hole")
                 .withColumnRenamed("id_vers", "id_well_hole")

    return dfTrn
}

// COMMAND ----------

import org.apache.spark.sql.{DataFrame}

def filterNonFutureDates(df: DataFrame): DataFrame = {
  val today = current_date()

  df.filter(to_date(col("fec_production_day")) <= today)
}


// COMMAND ----------

var dlInsert = "N/A"
var dlMsg = ""




var df: DataFrame  = null

try {
    
    val dfOper = transformOper()

    val dfProd = transformStat()

    val dfWellHole = transformWellHole(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hole_unc")))

    df = joinOperProd(dfOper, dfProd)

    df = joinWellHole(df, dfWellHole)

    df = selectFields(df).dropDuplicates("__calculated_key__")

    val (targetDf, targetDelta) = getDeltaTable(df, targetFinal)
    applyChange(targetFinal, df)
 
    optimizeDeltaFun(targetDelta, getObject(targetFinal("target_object").toString) ,auxParamsFinal("dayOfWeek").toString)
    
    df= df
    
    df= filterNonFutureDates(df)

    val countWrites = df.count()

   
    println(s"Process complete with: $countWrites written rows")
} catch {
    case e: Throwable => 
                error = e.toString
                println(error)
                LogHelper().logEndKO(error)
}



// COMMAND ----------

val result = s"""{"error": "$error"}"""

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

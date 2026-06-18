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

val selectFieldsSeq = Seq("fec_production_day", "id_vers_tank", "id_tank", "id_area", "id_fcty_class_1", "id_oper_route", "id_prod_unit", "id_storage", "fec_create_date", "fec_update_date")

val selectAttributesUnitsSeq = Seq("ind_grs_vol_wat_bbl", "ind_grs_vol_wat_m3", "ind_std_grs_vol_oil_stb", "ind_std_grs_vol_oil_sm3", "ind_std_net_vol_oil_stb", "ind_std_net_vol_oil_sm3")


def selectFields (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    val selectFields = selectFieldsSeq ++ selectAttributesUnitsSeq

    selectAttributesUnitsSeq.foreach { field =>
        val fieldWithoutInd = field.substring(4)

        if (dfTrn.columns.contains(fieldWithoutInd)) {
            dfTrn = dfTrn.withColumnRenamed(fieldWithoutInd, field)
        }
    }
    
    selectAttributesUnitsSeq.foreach { field =>
        if (!dfTrn.columns.contains(field)) {
            println("Field not exist:" + field)
            dfTrn = dfTrn.withColumn(field, lit(null).cast("Double"))
        }
    }

    dfTrn = dfTrn.select(selectFields.map(col): _*)
   
    return dfTrn

}



def joinWithMst(df1: DataFrame, df2: DataFrame, fieldsPartition: Seq[String], df1Filter1:String, df2Filter1:String, df1Filter2:String, df2Filter2:String, selectFields: Seq[String], joinType: String): DataFrame = {
    
    val windowSpec = Window.partitionBy(fieldsPartition.map(col): _*).orderBy(df2(df2Filter2).desc)
    
    val joinedDF = df1.join(df2, df1(df1Filter1) === df2(df2Filter1) && df2(df2Filter2) <= df1(df1Filter2), joinType)
    val windowDF = joinedDF.withColumn("row_number",row_number().over(windowSpec)).filter("row_number == 1")

    val selectedDF = windowDF.select((df1.columns.map(df1(_)) ++ selectFields.map(df2(_))):_*)
    selectedDF

}

def joinWithDim(df1: DataFrame, df2: DataFrame, fieldsPartition: Seq[String], df1Filter1:String, df2Filter1:String, df1Filter2:String, df2Filter2:String, joinType: String): DataFrame = {

    val windowSpec = Window.partitionBy(fieldsPartition.map(df1(_)): _*).orderBy((desc("id_vers")))
    
    val joinedDF = df1.join(df2, df1(df1Filter1) === df2(df2Filter1) && df2(df2Filter2) <= df1(df1Filter2), joinType).withColumn("id_vers", coalesce(df2("id_vers"), lit("-99")))
    val windowDF = joinedDF.withColumn("row_number",row_number().over(windowSpec)).filter("row_number == 1")

    val selectedDF = windowDF.select((df1.columns.map(df1(_)) ++ Seq("id_vers").map(col)):_*)
    selectedDF

}


def joinWithDimSelect(df1: DataFrame, df2: DataFrame, fieldsPartition: Seq[String], df1Filter1:String, df2Filter1:String, df1Filter2:String, df2Filter2:String, selectFields: Seq[String], joinType: String): DataFrame = {

    val windowSpec = Window.partitionBy(fieldsPartition.map(df1(_)): _*).orderBy((desc("id_vers")))
    
    val joinedDF = df1.join(df2, df1(df1Filter1) === df2(df2Filter1) && df2(df2Filter2) <= df1(df1Filter2), joinType).withColumn("id_vers", coalesce(df2("id_vers"), lit("-99")))
    val windowDF = joinedDF.withColumn("row_number",row_number().over(windowSpec)).filter("row_number == 1")

    val selectedDF = windowDF.select((df1.columns.map(df1(_)) ++ selectFields.map(col)):_*)
    selectedDF

}


// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------


var dlInsert = "N/A"
var dlMsg = ""
 

var df: DataFrame  = null

val dfTransactional = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_fac_d_tank_sing_wl"))

val dfMstTank = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_tank"))

val dfDimTank = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_tank"))
val dfDimArea = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_area"))
val dfDimFctyClass1 = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_fcty_class_1"))
val dfDimOperRoute = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_oper_route"))
val dfDimProdUnit = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_prod_unit"))
val dfDimStorage = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_storage"))
val dfDimTankUsage = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_tank_usage"))


try {
    
    df = joinWithMst(dfTransactional, dfMstTank,Seq("fec_production_day", "id_tank", "fec_create_date"), 
                                        "id_tank", "object_id", "fec_production_day", "daytime" ,
                                        Seq("op_area_id", "op_fcty_1_id", "cp_operator_route_id", "op_productionunit_id") ,
                                        "left")


    
    df = joinWithDimSelect(df, dfDimTank, Seq("fec_production_day", "id_tank", "fec_create_date"), "id_tank", "id_tank", "fec_production_day", "fec_start_date", Seq("id_vers"), "left")
    df = df.withColumnRenamed("id_vers", "id_vers_tank")

    df = joinWithDim(df, dfDimArea, Seq("fec_production_day", "id_tank", "fec_create_date"), "op_area_id", "id_area", "fec_production_day", "fec_start_date", "left")
    df = df.drop("op_area_id").withColumnRenamed("id_vers", "id_area")

    df = joinWithDim(df, dfDimFctyClass1, Seq("fec_production_day", "id_tank", "fec_create_date"), "op_fcty_1_id", "id_facility_class_1", "fec_production_day", "fec_start_date", "left")
    df = df.drop("op_fcty_1_id").withColumnRenamed("id_vers", "id_fcty_class_1")

    df = joinWithDim(df, dfDimOperRoute, Seq("fec_production_day", "id_tank", "fec_create_date"), "cp_operator_route_id", "id_operator_route", "fec_production_day", "fec_start_date", "left")
    df = df.drop("cp_operator_route_id").withColumnRenamed("id_vers", "id_oper_route")

    df = joinWithDim(df, dfDimProdUnit, Seq("fec_production_day", "id_tank", "fec_create_date"), "op_productionunit_id", "id_productunit", "fec_production_day", "fec_start_date", "left")
    df = df.drop("op_productionunit_id").withColumnRenamed("id_vers", "id_prod_unit")

    // cruce con la tank usage

    df = joinWithMst(df, dfDimTankUsage, Seq("fec_production_day", "id_tank", "fec_create_date"), "id_tank", "tank_id", "fec_production_day", "daytime", Seq("object_id"), "left")
    df = df.withColumnRenamed("object_id", "id_storage_aux")

    // contra storage
    df = joinWithDim(df, dfDimStorage, Seq("fec_production_day", "id_tank", "fec_create_date"), "id_storage_aux", "id_storage", "fec_production_day", "fec_start_date", "left")
    df = df.drop("id_storage_aux").withColumnRenamed("id_vers", "id_storage")


    

    df = selectFields(df)

    val dfFormatted = applyDDLFromTableToDF(df,getObject((target_object).toString))

    writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

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
// MAGIC

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

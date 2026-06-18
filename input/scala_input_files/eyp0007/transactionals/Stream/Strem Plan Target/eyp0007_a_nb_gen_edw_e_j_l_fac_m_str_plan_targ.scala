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

val selectFieldsSeq = Seq("fec_production_day", "id_vers_stream", "id_stream", "id_fcty_class_1", "id_col_point", "id_productionunit", "id_area", "id_operator_route", "id_product", "id_licence", "id_disposition_type", "des_comments", "fec_create_date", "fec_update_date")

val selectAttributesUnitsSeq = Seq("ind_cond_upa1_kstbpermth", "ind_cond_upa1_stbpermth", "ind_cond_upa2_kstbpermth", "ind_cond_upa2_stbpermth", "ind_cond_upa3_kstbpermth", "ind_cond_upa3_stbpermth", "ind_cond_upa4_kstbpermth", "ind_cond_upa4_stbpermth", "ind_cond_upa5_kstbpermth", "ind_cond_upa5_stbpermth", "ind_cond_upa6_kstbpermth", "ind_cond_upa6_stbpermth", "ind_cond_upa7_kstbpermth", "ind_cond_upa7_stbpermth", "ind_cond_upa8_kstbpermth", "ind_cond_upa8_stbpermth", "ind_cond_upa9_kstbpermth", "ind_cond_upa9_stbpermth", "ind_cond_upa10_kstbpermth", "ind_cond_upa10_stbpermth", "ind_cond_upa11_kstbpermth", "ind_cond_upa11_stbpermth", "ind_cond_upa12_kstbpermth", "ind_cond_upa12_stbpermth", "ind_gas_upa1_mmscfpermth", "ind_gas_upa1_kscfpermth", "ind_gas_upa2_mmscfpermth", "ind_gas_upa2_kscfpermth", "ind_gas_upa3_mmscfpermth", "ind_gas_upa3_kscfpermth", "ind_gas_upa4_mmscfpermth", "ind_gas_upa4_kscfpermth", "ind_gas_upa5_mmscfpermth", "ind_gas_upa5_kscfpermth", "ind_gas_upa6_mmscfpermth", "ind_gas_upa6_kscfpermth", "ind_gas_upa7_mmscfpermth", "ind_gas_upa7_kscfpermth", "ind_gas_upa8_mmscfpermth", "ind_gas_upa8_kscfpermth", "ind_gas_upa9_mmscfpermth", "ind_gas_upa9_kscfpermth", "ind_gas_upa10_mmscfpermth", "ind_gas_upa10_kscfpermth", "ind_gas_upa11_mmscfpermth", "ind_gas_upa11_kscfpermth", "ind_gas_upa12_mmscfpermth", "ind_gas_upa12_kscfpermth", "ind_ngl_upa1_kstbpermth", "ind_ngl_upa1_stbpermth", "ind_ngl_upa2_kstbpermth", "ind_ngl_upa2_stbpermth", "ind_ngl_upa3_kstbpermth", "ind_ngl_upa3_stbpermth", "ind_ngl_upa4_kstbpermth", "ind_ngl_upa4_stbpermth", "ind_ngl_upa5_kstbpermth", "ind_ngl_upa5_stbpermth", "ind_ngl_upa6_kstbpermth", "ind_ngl_upa6_stbpermth", "ind_ngl_upa7_kstbpermth", "ind_ngl_upa7_stbpermth", "ind_ngl_upa8_kstbpermth", "ind_ngl_upa8_stbpermth", "ind_ngl_upa9_kstbpermth", "ind_ngl_upa9_stbpermth", "ind_ngl_upa10_kstbpermth", "ind_ngl_upa10_stbpermth", "ind_ngl_upa11_kstbpermth", "ind_ngl_upa11_stbpermth", "ind_ngl_upa12_kstbpermth", "ind_ngl_upa12_stbpermth", "ind_oil_upa1_kstbpermth", "ind_oil_upa1_stbpermth", "ind_oil_upa2_kstbpermth", "ind_oil_upa2_stbpermth", "ind_oil_upa3_kstbpermth", "ind_oil_upa3_stbpermth", "ind_oil_upa4_kstbpermth", "ind_oil_upa4_stbpermth", "ind_oil_upa5_kstbpermth", "ind_oil_upa5_stbpermth", "ind_oil_upa6_kstbpermth", "ind_oil_upa6_stbpermth", "ind_oil_upa7_kstbpermth", "ind_oil_upa7_stbpermth", "ind_oil_upa8_kstbpermth", "ind_oil_upa8_stbpermth", "ind_oil_upa9_kstbpermth", "ind_oil_upa9_stbpermth", "ind_oil_upa10_kstbpermth", "ind_oil_upa10_stbpermth", "ind_oil_upa11_kstbpermth", "ind_oil_upa11_stbpermth", "ind_oil_upa12_kstbpermth", "ind_oil_upa12_stbpermth")// Incluir resto de attrbiutos-unidades


def selectFields (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    val selectFields = selectFieldsSeq ++ selectAttributesUnitsSeq

    selectAttributesUnitsSeq.foreach { field =>
        val fieldWithoutInd = field.substring(4).replaceFirst("rate_", "rate_mth_")

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


def breakDownToDailySplit(df: DataFrame, dateColumn: String, fieldsToSplit: Seq[String]): DataFrame = {

  val dfWithLastDayOfMonth = df.withColumn("last_day_of_month", last_day(col(dateColumn)))
  val dfWithDaysInMonth = dfWithLastDayOfMonth.withColumn("days_in_month", dayofmonth(col("last_day_of_month")))
  val breakDownDf = dfWithDaysInMonth.withColumn("day", explode(expr(s"sequence(1, days_in_month)")))
  

  val breakDownDfWithDivision = fieldsToSplit.foldLeft(breakDownDf)((df, colName) => {
    df.withColumn(colName, col(colName) / col("days_in_month"))
  })
  
  val breakDownDfFinal = breakDownDfWithDivision.withColumn(dateColumn, to_timestamp(date_add(col(dateColumn), col("day") - 1)))
  
  breakDownDfFinal.drop("last_day_of_month", "days_in_month", "day")
}



// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

val autoMerge = paramsFinal.getOrElse("spark.databricks.delta.schema.autoMerge.enabled", "false").toString.toBoolean

if(autoMerge) {spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", true)}

// COMMAND ----------


var dlInsert = "N/A"
var dlMsg = ""
 

var df: DataFrame  = null

val dfTransactional = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_fac_m_str_plan_targ_uc"))

val dfMstStrem = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_stream"))

val dfDimStream = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_stream"))
val dfDimFctyClass1 = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_fcty_class_1"))
val dfDimArea = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_area"))
val dfDimProdUnit = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_prod_unit"))
val dfDimOperRoute = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_oper_route"))
val dfDimCollecPoint  = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_collec_point"))
val dfDimLicence = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_licence"))
val dfDimProduct = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_product"))
val dfDimDispType = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_disp_type"))


try {
    

    df = joinWithMst(dfTransactional, dfMstStrem, Seq("fec_production_day", "id_stream"), 
                                        "id_stream", "object_id", "fec_production_day", "daytime" ,
                                        Seq("op_fcty_1_id", "op_area_id", "op_productionunit_id", "cp_operator_route_id", "cp_col_point_id", "mur_id", "product_id", "disposition_type_id") ,
                                        "left")



    df = joinWithDimSelect(df, dfDimStream, Seq("fec_production_day", "id_stream"), "id_stream", "id_stream", "fec_production_day", "fec_start_date", Seq("id_vers"), "left")
    df = df.withColumnRenamed("id_vers", "id_vers_stream")


    df = joinWithDim(df, dfDimFctyClass1, Seq("fec_production_day", "id_stream"), "op_fcty_1_id", "id_facility_class_1", "fec_production_day", "fec_start_date", "left")
    df = df.drop("op_fcty_1_id").withColumnRenamed("id_vers", "id_fcty_class_1")

    df = joinWithDim(df, dfDimArea, Seq("fec_production_day", "id_stream"), "op_area_id", "id_area", "fec_production_day", "fec_start_date", "left")
    df = df.drop("op_area_id").withColumnRenamed("id_vers", "id_area")

    df = joinWithDim(df, dfDimProdUnit, Seq("fec_production_day", "id_stream"), "op_productionunit_id", "id_productunit", "fec_production_day", "fec_start_date", "left")
    df = df.drop("op_productionunit_id").withColumnRenamed("id_vers", "id_productionunit")

    df = joinWithDim(df, dfDimOperRoute, Seq("fec_production_day", "id_stream"), "cp_operator_route_id", "id_operator_route", "fec_production_day", "fec_start_date", "left")
    df = df.drop("cp_operator_route_id").withColumnRenamed("id_vers", "id_operator_route")

    df = joinWithDim(df, dfDimCollecPoint, Seq("fec_production_day", "id_stream"), "cp_col_point_id", "id_collection_point", "fec_production_day", "fec_start_date", "left")
    df = df.drop("cp_col_point_id").withColumnRenamed("id_vers", "id_col_point")

    df = joinWithDim(df, dfDimLicence, Seq("fec_production_day", "id_stream"), "mur_id", "id_licence", "fec_production_day", "fec_start_date", "left")
    df = df.drop("mur_id").withColumnRenamed("id_vers", "id_licence")

    df = joinWithDim(df, dfDimProduct, Seq("fec_production_day", "id_stream"), "product_id", "id_product", "fec_production_day", "fec_start_date", "left")
    df = df.drop("product_id").withColumnRenamed("id_vers", "id_product")

    df = joinWithDim(df, dfDimDispType, Seq("fec_production_day", "id_stream"), "disposition_type_id", "id_disp_type", "fec_production_day", "fec_start_date", "left")
    df = df.drop("disposition_type_id").withColumnRenamed("id_vers", "id_disposition_type")

    df = selectFields(df)

    df = breakDownToDailySplit(df, "fec_production_day", selectAttributesUnitsSeq)

    df = df.withColumn("__calculated_key__", concat_ws("_", auxParamsFinal.get("keys").map(toScalaList).getOrElse(Seq.empty[String]).map(col): _*))

    val (targetDf, targetDelta) = getDeltaTable(df, targetFinal)
    applyChange(targetFinal, df)
 
    optimizeDeltaFun(targetDelta, getObject(targetFinal("target_object").toString) ,auxParamsFinal("dayOfWeek").toString)
  
    dlMsg = "Rows Inserted: " + df.count
    dlInsert = "OK"

} 
catch {
    case e: Throwable => 
                error = e.toString
                println(error)
                LogHelper().logEndKO(error)
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

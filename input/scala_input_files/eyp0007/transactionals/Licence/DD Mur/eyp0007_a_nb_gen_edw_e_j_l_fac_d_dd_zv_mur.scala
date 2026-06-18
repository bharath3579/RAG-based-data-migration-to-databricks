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

val selectFieldsSeq = Seq("fec_production_day","id_licence","id_mur","des_reserves_area","des_reservoir","ind_of_grs_condensate_bbl",
"ind_of_grs_gas_boe","ind_of_grs_gas_dissolved_boe","ind_of_grs_gas_dissolved_mscf","ind_of_grs_gas_mscf","ind_of_grs_ngl_bbl",
"ind_of_grs_oil_primary_bbl","ind_of_net_gas_boe","ind_of_net_gas_dissolved_boe","ind_of_net_condensate_bbl",
"ind_of_net_gas_dissolved_mscf","ind_of_net_gas_mscf","ind_of_net_ngl_bbl","ind_of_net_oil_primary_bbl","ind_of_wi_condensate_bbl",
"ind_of_wi_gas_boe","ind_of_wi_gas_dissolved_boe","ind_of_wi_gas_dissolved_mscf","ind_of_wi_gas_mscf","ind_of_wi_ngl_bbl",
"ind_of_wi_oil_primary_bbl","ind_prov_grs_condensate_bbl","ind_prov_grs_gas_boe","ind_prov_grs_gas_dissolved_boe","ind_prov_grs_gas_dissolve_mscf",
"ind_prov_grs_gas_mscf","ind_prov_grs_ngl_bbl","ind_prov_grs_oil_primary_bbl","ind_prov_net_condensate_bbl","ind_prov_net_gas_boe",
"ind_prov_net_gas_dissolved_boe","ind_prov_net_gas_dissolve_mscf","ind_prov_wi_gas_dissolved_boe","ind_prov_wi_gas_dissolved_mscf",
"ind_prov_net_gas_mscf","ind_prov_net_ngl_bbl","ind_prov_net_oil_primary_bbl","ind_prov_wi_condensate_bbl","ind_prov_wi_gas_boe",
"ind_prov_wi_gas_mscf","ind_prov_wi_ngl_bbl","ind_prov_wi_oil_primary_bbl", "id_area", "id_facility_class_1", "id_productionunit", "cod_object", "cod_vers_object")


def selectFields (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.select(selectFieldsSeq.map(col): _*)

   
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


var dlInsert = "N/A"
var dlMsg = ""
 

var df: DataFrame  = null

val dfTransactional =  getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_fac_d_dd_zv_mur")) //MUR


val dfDimMstWell = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_well"))
val dfDimMstFctyClass1 = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_fcty_class_1"))
val dfDimMstStream = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_stream"))
val dfDimLicence = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_licence"))
val dfDimArea = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_area"))
val dfDimFctyClass1 = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_fcty_class_1"))
val dfDimProdUnit = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_prod_unit"))
val dfDimWell = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well")).selectExpr("id_vers", "id_well", "fec_start_date", "fec_end_date")
val dfDimStream = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_stream")).selectExpr("id_vers", "id_stream", "fec_start_date", "fec_end_date")


try {

    var dfW: DataFrame = null
    var dfS: DataFrame = null
    var dfF: DataFrame = null

    // MASTER JOINS
    dfW = joinWithMst(dfTransactional, dfDimMstWell, Seq("fec_production_day", "id_mur", "objectId"), "objectId", "object_id", "fec_production_day", "daytime", Seq("op_area_id", "op_productionunit_id", "op_fcty_1_id"), "inner")

    dfS = joinWithMst(dfTransactional, dfDimMstStream, Seq("fec_production_day", "id_mur", "objectId"), "objectId", "object_id", "fec_production_day", "daytime", Seq("op_area_id", "op_productionunit_id", "op_fcty_1_id"), "inner")

    dfF = joinWithMst(dfTransactional, dfDimMstFctyClass1, Seq("fec_production_day", "id_mur", "objectId"), "objectId", "object_id", "fec_production_day", "daytime", Seq("op_area_id", "op_productionunit_id", "object_id"), "inner")
    dfF = dfF.withColumnRenamed("object_id", "op_fcty_1_id")

    // MASTER UNION
    df = dfW.unionByName(dfS)

    df = df.unionByName(dfF)
    
    // JOIN WITH DIMS
    df = joinWithDimSelect(df, dfDimLicence, Seq("fec_production_day", "id_mur", "objectId"), "id_mur", "id_licence", "fec_production_day", "fec_start_date", Seq("id_vers"), "left")
    df = df.withColumnRenamed("id_vers", "id_licence")

    df = joinWithDimSelect(df, dfDimArea, Seq("fec_production_day", "id_mur", "objectId"), "op_area_id", "id_area", "fec_production_day", "fec_start_date", Seq("id_vers"), "left")
    df = df.withColumnRenamed("id_vers", "id_area")

    df = joinWithDimSelect(df, dfDimFctyClass1, Seq("fec_production_day", "id_mur", "objectId"), "op_fcty_1_id", "id_facility_class_1", "fec_production_day", "fec_start_date", Seq("id_vers"), "left")
    df = df.withColumnRenamed("id_vers", "id_facility_class_1")

    df = joinWithDimSelect(df, dfDimProdUnit, Seq("fec_production_day", "id_mur", "objectId"), "op_productionunit_id", "id_productunit", "fec_production_day", "fec_start_date", Seq("id_vers"), "left")
    df = df.withColumnRenamed("id_vers", "id_productionunit")
    val df27 = df.count()


    val joinWithWell = df.join(dfDimWell, (df("objectId") === dfDimWell("id_well")) && (df("fec_production_day").between(dfDimWell("fec_start_date"), coalesce(dfDimWell("fec_end_date"), current_timestamp))), "left")
    .select(df("*"), dfDimWell("id_vers").as("cod_vers_object"))
    val df28 = joinWithWell.count()
    println(df28)
    //display(joinWithWell)

   /*// Convertir la columna de fecha a tipo date
val dfConFecha = joinWithWell.withColumn("fecha_prod", col("fec_production_day").cast("date"))

// Filtrar y agregar los valores, reemplazando nulls por 0 en cada suma
val dfResult = dfConFecha
  .filter(col("id_area").isin(
    "19700101AD40998D666F4F878518E069D0119C1B",
    "1970010105BC1E780AD14957BFEFAF14DEF91EF3"
  ))
  .filter(col("fecha_prod").between("2023-01-01", "2023-01-31"))
  .agg(
    sum(coalesce(col("ind_prov_net_condensate_bbl"), lit(0))).alias("sum_ind_prov_net_condensate_bbl"),
    sum(coalesce(col("ind_prov_net_gas_boe"), lit(0))).alias("sum_ind_prov_net_gas_boe"),
    sum(coalesce(col("ind_prov_net_gas_dissolved_boe"), lit(0))).alias("sum_ind_prov_net_gas_dissolved_boe"),
    sum(coalesce(col("ind_prov_net_ngl_bbl"), lit(0))).alias("sum_ind_prov_net_ngl_bbl"),
    sum(coalesce(col("ind_prov_net_oil_primary_bbl"), lit(0))).alias("sum_ind_prov_net_oil_primary_bbl")
  )
  display(dfResult)*/

    val joinWithStream = df.join(dfDimStream, (df("objectId") === dfDimStream("id_stream")) && (df("fec_production_day").between(dfDimStream("fec_start_date"), coalesce(dfDimStream("fec_end_date"), current_timestamp))), "left")
        .select(df("*"), dfDimStream("id_vers").as("cod_vers_object"))

    //display(joinWithStream)

    //val joinWithWell = joinWithDimSelect(df, dfDimFctyClass1, Seq("fec_production_day", "id_mur", "objectId"), "op_fcty_1_id", "id_facility_class_1", "fec_production_day", "fec_start_date", Seq("id_vers"), "left")
    
    /*df = joinWithWell.union(joinWithStream).distinct()
    .withColumnRenamed("objectId", "cod_object")
    df= df.dropDuplicates*/

    df= joinWithWell.withColumnRenamed("objectId", "cod_object")

    /*// Convertir la columna de fecha a tipo date
val dfConFecha2 = df.withColumn("fecha_prod", col("fec_production_day").cast("date"))

// Filtrar y agregar los valores, reemplazando nulls por 0 en cada suma
val dfResult2 = dfConFecha2
  .filter(col("id_area").isin(
    "19700101AD40998D666F4F878518E069D0119C1B",
    "1970010105BC1E780AD14957BFEFAF14DEF91EF3"
  ))
  .filter(col("fecha_prod").between("2023-01-01", "2023-01-31"))
  .agg(
    sum(coalesce(col("ind_prov_net_condensate_bbl"), lit(0))).alias("sum_ind_prov_net_condensate_bbl"),
    sum(coalesce(col("ind_prov_net_gas_boe"), lit(0))).alias("sum_ind_prov_net_gas_boe"),
    sum(coalesce(col("ind_prov_net_gas_dissolved_boe"), lit(0))).alias("sum_ind_prov_net_gas_dissolved_boe"),
    sum(coalesce(col("ind_prov_net_ngl_bbl"), lit(0))).alias("sum_ind_prov_net_ngl_bbl"),
    sum(coalesce(col("ind_prov_net_oil_primary_bbl"), lit(0))).alias("sum_ind_prov_net_oil_primary_bbl")
  )
  display(dfResult2)*/


    df = selectFields(df).withColumn("val_dummy_column_partition", lit(1))
    val dfFormatted = applyDDLFromTableToDF(df,getObject(targetFinal("target_object").toString))
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
// MAGIC # Informar del resultado

// COMMAND ----------

val result = "OK"

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

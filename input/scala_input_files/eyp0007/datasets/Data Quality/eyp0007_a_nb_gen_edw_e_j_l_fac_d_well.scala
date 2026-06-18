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
// MAGIC # Funcion de lectura/Escritura del Dataset

// COMMAND ----------

// MAGIC %md
// MAGIC # Código que implementa la lógica necesaria

// COMMAND ----------

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._




def joinWithMst(df1: DataFrame, df2: DataFrame, df1Filter1:String, df2Filter1:String, df1Filter2:String, df2Filter2:String, selectFields: Seq[String], joinType: String): DataFrame = {
    
    val joinedDF = df1.join(df2, df1(df1Filter1) === df2(df2Filter1) && df2(df2Filter2) === df1(df1Filter2), joinType)
   
    val selectedDF = joinedDF.select((df1.columns.map(df1(_)) ++ selectFields.map(df2(_))):_*)
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

def joinWithDimSelectCDS(df1: DataFrame, df2: DataFrame, fieldsPartition: Seq[String], df1Filter1:String, df2Filter1:String, selectFields: Seq[String], joinType: String): DataFrame = {

    val windowSpec = Window.partitionBy(fieldsPartition.map(df1(_)): _*).orderBy((desc("id_vers")))
    
    val joinedDF = df1.join(df2, df1(df1Filter1) === df2(df2Filter1), joinType).withColumn("id_vers", coalesce(df2("id_vers"), lit("-99")))
    val windowDF = joinedDF.withColumn("row_number",row_number().over(windowSpec)).filter("row_number == 1")

    val selectedDF = windowDF.select((df1.columns.map(df1(_)) ++ selectFields.map(col)):_*)
    selectedDF
}

def joinWithDimCDS(df1: DataFrame, df2: DataFrame, fieldsPartition: Seq[String], df1Filter1:String, df2Filter1:String, joinType: String): DataFrame = {

    val windowSpec = Window.partitionBy(fieldsPartition.map(df1(_)): _*).orderBy((desc("id_vers")))
    
    val joinedDF = df1.join(df2, df1(df1Filter1) === df2(df2Filter1), joinType).withColumn("id_vers", coalesce(df2("id_vers"), lit("-99")))
    val windowDF = joinedDF.withColumn("row_number",row_number().over(windowSpec)).filter("row_number == 1")

    val selectedDF = windowDF.select((df1.columns.map(df1(_)) ++ Seq("id_vers").map(col)):_*)
    selectedDF

}

def toTranspose(df: DataFrame, columnsFixed: Seq[String], columnsTransposed: Seq[String]): DataFrame = {
  val transposeDf = df.select(
    (columnsFixed.map(col) ++
    Seq(expr(s"stack(${columnsTransposed.length}," + 
      columnsTransposed.map(colName => s"'$colName', $colName").mkString(", ") + ") as (attribute, value)"))): _*
  ).select((columnsFixed.map(col) ++ Seq(col("attribute"), col("value"))): _*)
  transposeDf
}

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------



var dlInsert = "N/A"
var dlMsg = ""
 

var df: DataFrame  = null

val dfMstWell = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_well"))
2
val dfDimWell = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well")).drop("id_commercial_entity", "id_fcty_1", "id_well_hookup", "id_well_hole", "id_vers_well_hole")
val dfDimComEntity = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_com_entity"))
val dfDimFctyClass1 = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_fcty_class_1"))
val dfDimWellHookup = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hookup"))
val dfDimWellHole = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hole"))
val dfDimArea = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_area"))
val dfDimProdUnit = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_prod_unit"))
val dfDimGeoArea = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_geo_area"))
val dfDimFieldCds = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_field_cds"))
val dfDimField = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_field"))
val dfDimBasin = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_basin"))
val dfDimOperRoute = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_oper_route"))
val dfDimCollecPoint  = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_collec_point"))
val dfDimLicence = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_licence"))


try {
    

    df = joinWithMst(dfDimWell, dfMstWell, 
                                        "id_well", "object_id", "fec_start_date", "daytime" ,
                                        Seq("commercial_entity_id", "op_fcty_1_id", "op_well_hookup_id", "well_hole_id", "op_area_id", "op_productionunit_id", "geo_area_id" , "geo_field_id", "cp_operator_route_id", "cp_col_point_id", "mur_id") ,
                                        "inner")
    df = df.withColumnRenamed("id_well", "object_id").withColumnRenamed("id_vers", "id_well")



    df = joinWithDim(df, dfDimComEntity, Seq("id_well"), "commercial_entity_id", "id_commercial_entity", "fec_start_date", "fec_start_date", "left")
    df = df.drop("commercial_entity_id").withColumnRenamed("id_vers", "id_commercial_entity")
    
   
     

    df = joinWithDimSelect(df, dfDimFctyClass1, Seq("id_well"), "op_fcty_1_id", "id_facility_class_1", "fec_start_date", "fec_start_date" , Seq("id_vers", "des_facility_class_1" ), "left")
    df = df.drop("op_fcty_1_id").withColumnRenamed("id_vers", "id_facility_class_1").withColumn("des_facility_class_1", coalesce(col("des_facility_class_1"), lit("Not available")))


    df = joinWithDim(df, dfDimWellHookup, Seq("id_well"), "op_well_hookup_id", "id_well_hookup", "fec_start_date", "fec_start_date", "left")
    df = df.drop("op_well_hookup_id").withColumnRenamed("id_vers", "id_well_hookup")

    df = joinWithDim(df, dfDimWellHole, Seq("id_well"), "well_hole_id", "id_well_hole", "fec_start_date", "fec_start_date", "left")
    df = df.drop("well_hole_id").withColumnRenamed("id_vers", "id_well_hole")

    df = joinWithDimSelect(df, dfDimArea,  Seq("id_well"), "op_area_id", "id_area", "fec_start_date", "fec_start_date" , Seq("id_vers", "des_area"), "left")
    df = df.drop("op_area_id").withColumnRenamed("id_vers", "id_area").withColumn("des_area", coalesce(col("des_area"), lit("Not available")))

    df = joinWithDimSelect(df, dfDimProdUnit, Seq("id_well"), "op_productionunit_id", "id_productunit", "fec_start_date", "fec_start_date" , Seq("id_vers", "des_productunit"), "left")
    df = df.drop("op_productionunit_id").withColumnRenamed("id_vers", "id_productionunit").withColumn("des_productunit", coalesce(col("des_productunit"), lit("Not available")))

    df = joinWithDim(df, dfDimGeoArea, Seq("id_well"), "geo_area_id", "id_geo_area", "fec_start_date", "fec_start_date", "left")
    df = df.drop("geo_area_id").withColumnRenamed("id_vers", "id_geo_area")

    df = joinWithDimSelectCDS(df, dfDimFieldCds, Seq("id_well"), "id_field_cds", "id_field", Seq("id_vers", "id_basin"), "left")
    df = df.drop("id_field_cds").withColumnRenamed("id_vers", "id_field_cds")

    df = joinWithDim(df, dfDimField, Seq("id_well"), "geo_field_id", "id_field", "fec_start_date", "fec_start_date", "left")
    df = df.drop("geo_field_id").withColumnRenamed("id_vers", "id_field")

    df = joinWithDimCDS(df, dfDimBasin, Seq("id_well"), "id_basin", "id_basin", "left")
    df = df.drop("id_basin").withColumnRenamed("id_vers", "id_basin")

    df = joinWithDim(df, dfDimOperRoute, Seq("id_well"), "cp_operator_route_id", "id_operator_route", "fec_start_date", "fec_start_date", "left")
    df = df.drop("cp_operator_route_id").withColumnRenamed("id_vers", "id_operator_route")

    df = joinWithDim(df, dfDimCollecPoint, Seq("id_well"), "cp_col_point_id", "id_collection_point", "fec_start_date", "fec_start_date", "left")
    df = df.drop("cp_col_point_id").withColumnRenamed("id_vers", "id_col_point")

    df = joinWithDimCDS(df, dfDimLicence, Seq("id_well"), "mur_id", "id_licence", "left")
    df = df.drop("mur_id").withColumnRenamed("id_vers", "id_licence")

    df = df.withColumnRenamed("object_id", "id_ec_object")
           .withColumnRenamed("des_well", "des_object_name")
           .drop("fec_create_date", "fec_update_date", "val_dummy_column_partition")


     val dfCols: Seq[String] = df.columns.toSeq 


    val dfFixedCols: Seq[String] = Seq("fec_start_date", "fec_end_date", "id_ec_object","id_well", "des_object_name", 
    "id_commercial_entity", "id_facility_class_1", "des_facility_class_1","id_well_hookup", "id_well_hole", 
    "id_area", "des_area","id_productionunit", "des_productunit","id_geo_area", "id_field_cds", "id_field",  
    "id_basin", "id_operator_route", "id_col_point", "id_licence")

    val dfTransposedCols : Seq[String] = dfCols.diff(dfFixedCols)

    val dfTranspose = toTranspose(df, dfFixedCols, dfTransposedCols)

    // writeDataFrame(dfTranspose, targetFinal)
    val dfFormatted = applyDDLFromTableToDF(dfTranspose,getObject(targetFinal("target_object").toString))
    writeWrapper(sourceDF=dfFormatted,conf=targetFinal,prefix="target",operationName = target_operation_name,optimize=optimizeDelta)
    

    dlMsg = "Rows Inserted: " + dfTranspose.count
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


val result = s"""{"error": "$error","dlInsert": { "result": "$dlInsert", "msg": "$dlMsg" }}"""

if (uniqueKey.nonEmpty && step > 0)
    ControlHelper().updateControlTable(spark, uniqueKey, step, s"$name: $result")

if (!error.isEmpty || dlInsert.equals("KO")) {
    LogHelper().logError(s"$name: $result")
    throw new Exception(s"$name: $result")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

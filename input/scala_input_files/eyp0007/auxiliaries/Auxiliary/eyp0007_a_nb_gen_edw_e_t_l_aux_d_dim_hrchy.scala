// Databricks notebook source
dbutils.widgets.text("applicationName", "")
val applicationName: String = dbutils.widgets.get("applicationName")
dbutils.widgets.text("uuid", "N/A")
val uuid: String = dbutils.widgets.get("uuid")
dbutils.widgets.text("parentUid", "N/A")
val puid: String = dbutils.widgets.get("parentUid")
dbutils.widgets.text("step", "0")
var step: Int = dbutils.widgets.get("step").toInt

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
dbutils.widgets.text("digitalCase", "eyp0007")
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



var dlInsert = "N/A"
var dlMsg = ""
 

var dfWell:DataFrame  = null
var dfStream:DataFrame  = null
var dfChoke:DataFrame  = null
var dfWellHole: DataFrame = null 
var dfFinal:DataFrame = null 

val dfMstWell = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_well"))
                                                        .withColumn("id_well", 
                                                        concat(date_format(col("daytime"), "yyyyMMdd"), col("object_id")))
                                                        .withColumnRenamed("name", "des_well")
                                                        .select("daytime", "id_well", "des_well", "op_area_id", "op_productionunit_id", "op_well_hookup_id", "op_fcty_1_id", "well_hole_id")

val dfMstStream = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_stream")).withColumn("id_stream", 
                                                        concat(date_format(col("daytime"), "yyyyMMdd"), col("object_id")))
                                                        .withColumnRenamed("name", "des_stream")
                                                        .select("daytime", "id_stream", "des_stream", "op_area_id", "op_productionunit_id", "op_fcty_1_id")
                                                        

val dfMstChoke = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_choke_model")).withColumn("id_choke_model", 
                                                        concat(date_format(col("daytime"), "yyyyMMdd"), col("object_id")))
                                                        .withColumnRenamed("name", "des_choke_model")
                                                        .select("daytime", "id_choke_model", "des_choke_model", "op_area_id", "op_productionunit_id", "op_fcty_1_id")


val dfMstWellHole = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hole_unc")).filter(col("id_vers") =!= "-99")
                                                        .withColumnRenamed("id_well_hole", "well_hole_id")
                                                        .withColumnRenamed("id_vers", "id_well_hole")
                                                        .select("id_well_hole", "des_well_hole", "well_hole_id")                                      


val dfDimFctyClass1 = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_fcty_class_1"))
val dfDimWellHookup = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hookup"))
val dfDimArea = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_area"))
val dfDimProdUnit = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_prod_unit"))


try {
    
    
    //aux Well
    
    dfWell = joinWithDimSelect(dfMstWell, dfDimWellHookup,
                                      Seq("id_well"), "op_well_hookup_id", "id_well_hookup", "daytime", "fec_start_date" , Seq("id_vers", "des_well_hookup" ), "left")                  
    dfWell = dfWell.drop("op_well_hookup_id").withColumnRenamed("id_vers", "id_well_hookup").withColumn("des_well_hookup", coalesce(col("des_well_hookup"), lit("Not available")))


    dfWell = joinWithDimSelect(dfWell, dfDimFctyClass1, Seq("id_well"), "op_fcty_1_id", "id_facility_class_1", "daytime", "fec_start_date" , Seq("id_vers", "des_facility_class_1" ), "left")
    dfWell = dfWell.drop("op_fcty_1_id").withColumnRenamed("id_vers", "id_facility_class_1").withColumn("des_facility_class_1", coalesce(col("des_facility_class_1"), lit("Not available")))


    dfWell = joinWithDimSelect(dfWell, dfDimArea,  Seq("id_well"), "op_area_id", "id_area", "daytime", "fec_start_date" , Seq("id_vers", "des_area"), "left")
    dfWell = dfWell.drop("op_area_id").withColumnRenamed("id_vers", "id_area").withColumn("des_area", coalesce(col("des_area"), lit("Not available")))

    dfWell = joinWithDimSelect(dfWell, dfDimProdUnit, Seq("id_well"), "op_productionunit_id", "id_productunit", "daytime", "fec_start_date" , Seq("id_vers", "des_productunit"), "left")
    dfWell = dfWell.drop("op_productionunit_id").withColumnRenamed("id_vers", "id_productionunit").withColumn("des_productunit", coalesce(col("des_productunit"), lit("Not available")))

    //aux stream

    dfStream = joinWithDimSelect(dfMstStream, dfDimFctyClass1, Seq("id_stream"), "op_fcty_1_id", "id_facility_class_1", "daytime", "fec_start_date" , Seq("id_vers", "des_facility_class_1" ), "left")
    dfStream = dfStream.drop("op_fcty_1_id").withColumnRenamed("id_vers", "id_facility_class_1").withColumn("des_facility_class_1", coalesce(col("des_facility_class_1"), lit("Not available")))
   
    dfStream = joinWithDimSelect(dfStream, dfDimArea,  Seq("id_stream"), "op_area_id", "id_area", "daytime", "fec_start_date" , Seq("id_vers", "des_area"), "left")
    dfStream = dfStream.drop("op_area_id").withColumnRenamed("id_vers", "id_area").withColumn("des_area", coalesce(col("des_area"), lit("Not available")))

    dfStream = joinWithDimSelect(dfStream, dfDimProdUnit, Seq("id_stream"), "op_productionunit_id", "id_productunit", "daytime", "fec_start_date" , Seq("id_vers", "des_productunit"), "left")
    dfStream = dfStream.drop("op_productionunit_id").withColumnRenamed("id_vers", "id_productionunit").withColumn("des_productunit", coalesce(col("des_productunit"), lit("Not available")))

    //aux well_hole

    val dfWellaux = dfWell.select("well_hole_id", "id_well_hookup", "des_well_hookup", "id_facility_class_1", "des_facility_class_1", "id_area", "des_area", "id_productionunit", "des_productunit")
                          .dropDuplicates("well_hole_id")
    dfWellHole = dfMstWellHole.join(dfWellaux, Seq("well_hole_id"), "left").drop("well_hole_id")
                              .withColumn("des_facility_class_1", coalesce(col("des_facility_class_1"), lit("Not available")))
                              .withColumn("des_area", coalesce(col("des_area"), lit("Not available")))
                              .withColumn("des_productunit", coalesce(col("des_productunit"), lit("Not available")))
                              .withColumn("id_facility_class_1", coalesce(col("id_facility_class_1"), lit("-99")))
                              .withColumn("id_area", coalesce(col("id_area"), lit("-99")))
                              .withColumn("id_productionunit", coalesce(col("id_productionunit"), lit("-99")))

   //aux choke model

    dfChoke = joinWithDimSelect(dfMstChoke, dfDimFctyClass1, Seq("id_choke_model"), "op_fcty_1_id", "id_facility_class_1", "daytime", "fec_start_date" , Seq("id_vers", "des_facility_class_1" ), "left")
    dfChoke = dfChoke.drop("op_fcty_1_id").withColumnRenamed("id_vers", "id_facility_class_1").withColumn("des_facility_class_1", coalesce(col("des_facility_class_1"), lit("Not available")))
    
    dfChoke = joinWithDimSelect(dfChoke, dfDimArea,  Seq("id_choke_model"), "op_area_id", "id_area", "daytime", "fec_start_date" , Seq("id_vers", "des_area"), "left")
    dfChoke = dfChoke.drop("op_area_id").withColumnRenamed("id_vers", "id_area").withColumn("des_area", coalesce(col("des_area"), lit("Not available")))
  
    dfChoke = joinWithDimSelect(dfChoke, dfDimProdUnit, Seq("id_choke_model"), "op_productionunit_id", "id_productunit", "daytime", "fec_start_date" , Seq("id_vers", "des_productunit"), "left")
    dfChoke = dfChoke.drop("op_productionunit_id").withColumnRenamed("id_vers", "id_productionunit").withColumn("des_productunit", coalesce(col("des_productunit"), lit("Not available")))
  
  //union de DF's

    dfFinal = dfWell.unionByName(dfStream, allowMissingColumns=true)
    dfFinal = dfFinal.unionByName(dfChoke, allowMissingColumns=true)
    dfFinal = dfFinal.unionByName(dfWellHole, allowMissingColumns=true)
   
    val dfWrite = dfFinal.select("id_productionunit", "des_productunit", "id_area", "des_area", "id_facility_class_1", "des_facility_class_1", "id_well_hookup", "des_well_hookup",
                                 "id_well_hole" ,"des_well_hole","id_well", "des_well", "id_stream", "des_stream", "id_choke_model", "des_choke_model")
                         .withColumn("id_final", 
                            when(col("id_well").isNotNull, col("id_well"))
                            .when(col("id_stream").isNotNull, col("id_stream"))
                            .when(col("id_well_hole").isNotNull, col("id_well_hole"))
                            .otherwise(col("id_choke_model"))
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

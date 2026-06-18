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

val selectFieldsSeq = Seq("fec_production_day","id_vers_well","id_well","id_commercial_entity","id_facility_class_1",
"id_well_hookup","id_well_hole","id_area","id_productionunit","id_geo_area","id_field_cds","id_field","id_basin",
"id_operator_route","id_col_point","id_licence","fec_start_production_date","fec_end_production_date","fec_end_production_day",
"des_comments","id_event_no","id_sap_pm_notif","id_scheduled","ind_hrs_total","des_event_category_code","des_event_reason_code",
"des_event_system_code","ind_hours","des_object_type","des_root_cause","ind_cond_event_loss_sm3","ind_cond_event_loss_stb",
"ind_gas_event_loss_kscf","ind_oil_event_loss_sm3","ind_oil_event_loss_stb","fec_create_date","fec_update_date", "cod_commentity_ec_id",
"cod_fcty_ec_id", "cod_whook_ec_id", "cod_wellhole_ec_id", "cod_produnit_ec_id", "cod_geoarea_ec_id", "cod_field_cds_id", "cod_field_ec_id",
"cod_basin_cds_id", "cod_oprout_ec_id", "cod_colpoint_ec_id", "cod_licence_ec_id")


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

val notNullColumns = Seq(
  "fec_production_day",
  "id_vers_well",
  "id_well",
  "id_commercial_entity",
  "id_facility_class_1",
  "id_well_hookup",
  "id_well_hole",
  "id_area",
  "id_productionunit",
  "id_geo_area",
  "id_field_cds",
  "id_field",
  "id_basin",
  "id_operator_route",
  "id_col_point",
  "id_licence",
  "id_event_no"
)

/** Sustituye NULLs solo en columnas NOT NULL, sin crear columnas nuevas.
  * String  -> "ValNULL"
  * Date    -> 1900-01-01
  * Timestamp/smalldatetime -> 1900-01-01 00:00:00
  * Numéricos -> -99 (casteado al tipo original)
  */
def sanitizeNotNulls(df: DataFrame): DataFrame = {
  val present = notNullColumns.filter(df.columns.contains) // por si alguna no está en df
  val nameToType = df.schema.fields.map(f => f.name -> f.dataType).toMap

  present.foldLeft(df) { (acc, name) =>
    nameToType.get(name) match {
      case Some(StringType) =>
        acc.withColumn(name, coalesce(col(name), lit("ValNULL")))
      case Some(DateType) =>
        acc.withColumn(name, coalesce(col(name), lit("1900-01-01").cast(DateType)))
      case Some(TimestampType) =>
        acc.withColumn(name, coalesce(col(name), lit("1900-01-01 00:00:00").cast(TimestampType)))
      case Some(t @ (_: ByteType | _: ShortType | _: IntegerType | _: LongType |
                     _: DoubleType | _: DoubleType | _: DecimalType)) =>
        acc.withColumn(name, coalesce(col(name), lit(-99).cast(t)))
      case _ =>
        // Tipos no previstos: no modificar
        acc
    }
  }
}


// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------


var dlInsert = "N/A"
var dlMsg = ""
 

var df: DataFrame  = null

val dfFacWellPeriodStatus = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_fac_d_tpl_bo_wl_def"))

val dfMstWell = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_well"))

val dfDimWell = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well"))
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
val df_ec_codes = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_aux_d_ec_codes")).withColumnRenamed("id_bu", "bu_ec_codes")

try {


    df = joinWithMst(dfFacWellPeriodStatus, dfMstWell, Seq("fec_production_day","id_well", "id_event_no"), "id_well", "object_id", "fec_production_day", "daytime" ,
                        Seq("commercial_entity_id", "op_fcty_1_id", "op_well_hookup_id", "well_hole_id", "op_area_id", "op_productionunit_id", "geo_area_id" , "geo_field_id", "cp_operator_route_id", "cp_col_point_id", "mur_id") , "left")

    df = df.as("transactional").join(
                            df_ec_codes.filter(col("code_type") === "DEFER_CAUSE_CAT").drop("created_date", "last_updated_date").dropDuplicates("code","code_text", "bu_ec_codes").as("ec_code"), 
                            col("transactional.des_event_reason_code") === col("ec_code.code") &&
                            col("transactional.id_bu") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("des_event_reason_code", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "des_event_reason_code")

    df = df.as("transactional").join(
                            df_ec_codes.filter(col("code_type") === "DEFER_CAUSE_GRP").drop("created_date", "last_updated_date").dropDuplicates("code","code_text", "bu_ec_codes").as("ec_code"), 
                            col("transactional.des_event_category_code") === col("ec_code.code") &&
                            col("transactional.id_bu") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("des_event_category_code", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "des_event_category_code")

    df = df.as("transactional").join(
                            df_ec_codes.filter(col("code_type") === "DEFER_SYS_GRP").drop("created_date", "last_updated_date").dropDuplicates("code","code_text", "bu_ec_codes").as("ec_code"), 
                            col("transactional.des_event_system_code") === col("ec_code.code") &&
                            col("transactional.id_bu") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("des_event_system_code", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "des_event_system_code")

    df = df.as("transactional").join(
                            df_ec_codes.filter(col("code_type") === "DEFER_ROOT_CAUSE").drop("created_date", "last_updated_date").dropDuplicates("code","code_text", "bu_ec_codes").as("ec_code"), 
                            col("transactional.des_root_cause") === col("ec_code.code") &&
                            col("transactional.id_bu") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("des_root_cause", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "des_root_cause")

    df = joinWithDimSelect(df, dfDimWell, Seq("fec_production_day","id_well", "id_event_no"), "id_well", "id_well", "fec_production_day", "fec_start_date", Seq("id_vers", "id_field_cds"), "left")
    df = df.withColumnRenamed("id_vers", "id_vers_well")



    df = joinWithDim(df, dfDimComEntity, Seq("fec_production_day","id_well", "id_event_no"), "commercial_entity_id", "id_commercial_entity", "fec_production_day", "fec_start_date", "left")
    df = df.drop("commercial_entity_id").withColumnRenamed("id_vers", "id_commercial_entity")

    df = joinWithDim(df, dfDimFctyClass1, Seq("fec_production_day","id_well", "id_event_no"), "op_fcty_1_id", "id_facility_class_1", "fec_production_day", "fec_start_date", "left")
    df = df.drop("op_fcty_1_id").withColumnRenamed("id_vers", "id_facility_class_1")

    df = joinWithDim(df, dfDimWellHookup, Seq("fec_production_day","id_well", "id_event_no"), "op_well_hookup_id", "id_well_hookup", "fec_production_day", "fec_start_date", "left")
    df = df.drop("op_well_hookup_id").withColumnRenamed("id_vers", "id_well_hookup")

    df = joinWithDim(df, dfDimWellHole, Seq("fec_production_day","id_well", "id_event_no"), "well_hole_id", "id_well_hole", "fec_production_day", "fec_start_date", "left")
    df = df.drop("well_hole_id").withColumnRenamed("id_vers", "id_well_hole")

    df = joinWithDim(df, dfDimArea, Seq("fec_production_day","id_well", "id_event_no"), "op_area_id", "id_area", "fec_production_day", "fec_start_date", "left")
    df = df.drop("op_area_id").withColumnRenamed("id_vers", "id_area")

    df = joinWithDim(df, dfDimProdUnit, Seq("fec_production_day","id_well", "id_event_no"), "op_productionunit_id", "id_productunit", "fec_production_day", "fec_start_date", "left")
    df = df.drop("op_productionunit_id").withColumnRenamed("id_vers", "id_productionunit")

    df = joinWithDim(df, dfDimGeoArea, Seq("fec_production_day","id_well", "id_event_no"), "geo_area_id", "id_geo_area", "fec_production_day", "fec_start_date", "left")
    df = df.drop("geo_area_id").withColumnRenamed("id_vers", "id_geo_area")

    df = joinWithDimSelect(df, dfDimFieldCds, Seq("fec_production_day","id_well", "id_event_no"), "id_field_cds", "id_field", "fec_production_day", "fec_update_date", Seq("id_vers", "id_basin"), "left")
    df = df.drop("id_field_cds").withColumnRenamed("id_vers", "id_field_cds")

    df = joinWithDim(df, dfDimField, Seq("fec_production_day","id_well", "id_event_no"), "geo_field_id", "id_field", "fec_production_day", "fec_start_date", "left")
    df = df.drop("geo_field_id").withColumnRenamed("id_vers", "id_field")

    df = joinWithDim(df, dfDimBasin, Seq("fec_production_day","id_well", "id_event_no"), "id_basin", "id_basin", "fec_production_day", "fec_update_date", "left")
    df = df.drop("id_basin").withColumnRenamed("id_vers", "id_basin")

    df = joinWithDim(df, dfDimOperRoute, Seq("fec_production_day","id_well", "id_event_no"), "cp_operator_route_id", "id_operator_route", "fec_production_day", "fec_start_date", "left")
    df = df.drop("cp_operator_route_id").withColumnRenamed("id_vers", "id_operator_route")

    df = joinWithDim(df, dfDimCollecPoint, Seq("fec_production_day","id_well", "id_event_no"), "cp_col_point_id", "id_collection_point", "fec_production_day", "fec_start_date", "left")
    df = df.drop("cp_col_point_id").withColumnRenamed("id_vers", "id_col_point")

    df = joinWithDim(df, dfDimLicence, Seq("fec_production_day","id_well", "id_event_no"), "mur_id", "id_licence", "fec_production_day", "fec_start_date", "left")
    df = df.drop("mur_id").withColumnRenamed("id_vers", "id_licence")

    df= df
  .withColumn("cod_commentity_ec_id", col("id_commercial_entity"))
  .withColumn("cod_fcty_ec_id", col("id_facility_class_1"))
  .withColumn("cod_whook_ec_id", col("id_well_hookup"))
  .withColumn("cod_wellhole_ec_id", col("id_well_hole"))
  .withColumn("cod_area_ec_id", col("id_area"))
  .withColumn("cod_produnit_ec_id", col("id_productionunit"))
  .withColumn("cod_geoarea_ec_id", col("id_geo_area"))
  .withColumn("cod_field_cds_id", col("id_field_cds"))
  .withColumn("cod_field_ec_id", col("id_field"))
  .withColumn("cod_basin_cds_id", col("id_basin"))
  .withColumn("cod_oprout_ec_id", col("id_operator_route"))
  .withColumn("cod_colpoint_ec_id", col("id_col_point"))
  .withColumn("cod_licence_ec_id", col("id_licence"))

  df= sanitizeNotNulls(df)

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

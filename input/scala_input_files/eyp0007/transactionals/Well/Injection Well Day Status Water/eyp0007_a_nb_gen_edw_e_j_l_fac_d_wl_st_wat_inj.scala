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

val selectFieldsSeq = Seq("fec_production_day", "id_vers_well", "id_well", "id_commercial_entity", "id_facility_class_1", "id_well_hookup", "id_well_hole", "id_area", "id_productionunit", "id_geo_area", "id_field_cds", "id_field", "id_basin", "id_operator_route", "id_col_point", "id_licence", "ind_avg_choke_size", "des_choke_uom", "des_inj_type", "ind_calc_on_stream_hrs", "des_well_status", "fec_create_date", "fec_update_date")

val selectAttributesUnitsSeq = Seq("ind_alloc_inj_vol_bbl", "ind_alloc_inj_vol_m3", "ind_alloc_inj_vol_sm3", "ind_alloc_inj_vol_stb", "ind_avg_annulus_press_barg", "ind_avg_annulus_press_psig", "ind_avg_wh_press_barg", "ind_avg_wh_press_psig", "ind_avg_wh_temp_c", "ind_avg_wh_temp_f", "ind_inj_vol_bbl", "ind_inj_vol_m3", "ind_inj_vol_sm3", "ind_inj_vol_stb")


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


var dlInsert = "N/A"
var dlMsg = ""
 

var df: DataFrame  = null

val dfTransactional = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_fac_d_wl_st_wat_inj_uc"))

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
    

    df = joinWithMst(dfTransactional, dfMstWell,Seq("fec_production_day", "id_well", "fec_update_date"), 
                                        "id_well", "object_id", "fec_production_day", "daytime" ,
                                        Seq("commercial_entity_id", "op_fcty_1_id", "op_well_hookup_id", "well_hole_id", "op_area_id", "op_productionunit_id", "geo_area_id" , "geo_field_id", "cp_operator_route_id", "cp_col_point_id", "mur_id") ,
                                        "left")

    df = df.as("transactional").join(
                            df_ec_codes.filter(col("code_type") === "INJ_TYPE").drop("created_date", "last_updated_date").dropDuplicates("code_text", "bu_ec_codes").as("ec_code"), 
                            col("transactional.des_inj_type") === col("ec_code.code") &&
                            col("transactional.id_bu_fac") === col("ec_code.bu_ec_codes"),
                            "left"
                            )
    df = df.drop("des_inj_type", "code_type", "code", "bu_ec_codes" )
    df = df.withColumnRenamed("code_text", "des_inj_type")

    df = joinWithDimSelect(df, dfDimWell, Seq("fec_production_day", "id_well", "fec_update_date"), "id_well", "id_well", "fec_production_day", "fec_start_date", Seq("id_vers", "id_field_cds"), "left")
    df = df.withColumnRenamed("id_vers", "id_vers_well")



    df = joinWithDim(df, dfDimComEntity, Seq("fec_production_day", "id_well", "fec_update_date"), "commercial_entity_id", "id_commercial_entity", "fec_production_day", "fec_start_date", "left")
    df = df.drop("commercial_entity_id").withColumnRenamed("id_vers", "id_commercial_entity")
    

    df = joinWithDim(df, dfDimFctyClass1, Seq("fec_production_day", "id_well", "fec_update_date"), "op_fcty_1_id", "id_facility_class_1", "fec_production_day", "fec_start_date", "left")
    df = df.drop("op_fcty_1_id").withColumnRenamed("id_vers", "id_facility_class_1")

    df = joinWithDim(df, dfDimWellHookup, Seq("fec_production_day", "id_well", "fec_update_date"), "op_well_hookup_id", "id_well_hookup", "fec_production_day", "fec_start_date", "left")
    df = df.drop("op_well_hookup_id").withColumnRenamed("id_vers", "id_well_hookup")

    df = joinWithDim(df, dfDimWellHole, Seq("fec_production_day", "id_well", "fec_update_date"), "well_hole_id", "id_well_hole", "fec_production_day", "fec_start_date", "left")
    df = df.drop("well_hole_id").withColumnRenamed("id_vers", "id_well_hole")

    df = joinWithDim(df, dfDimArea, Seq("fec_production_day", "id_well", "fec_update_date"), "op_area_id", "id_area", "fec_production_day", "fec_start_date", "left")
    df = df.drop("op_area_id").withColumnRenamed("id_vers", "id_area")

    df = joinWithDim(df, dfDimProdUnit, Seq("fec_production_day", "id_well", "fec_update_date"), "op_productionunit_id", "id_productunit", "fec_production_day", "fec_start_date", "left")
    df = df.drop("op_productionunit_id").withColumnRenamed("id_vers", "id_productionunit")

    df = joinWithDim(df, dfDimGeoArea, Seq("fec_production_day", "id_well", "fec_update_date"), "geo_area_id", "id_geo_area", "fec_production_day", "fec_start_date", "left")
    df = df.drop("geo_area_id").withColumnRenamed("id_vers", "id_geo_area")

    df = joinWithDimSelect(df, dfDimFieldCds, Seq("fec_production_day", "id_well", "fec_update_date"), "id_field_cds", "id_field", "fec_production_day", "fec_update_date", Seq("id_vers", "id_basin"), "left")
    df = df.drop("id_field_cds").withColumnRenamed("id_vers", "id_field_cds")

    df = joinWithDim(df, dfDimField, Seq("fec_production_day", "id_well", "fec_update_date"), "geo_field_id", "id_field", "fec_production_day", "fec_start_date", "left")
    df = df.drop("geo_field_id").withColumnRenamed("id_vers", "id_field")

    df = joinWithDim(df, dfDimBasin, Seq("fec_production_day", "id_well", "fec_update_date"), "id_basin", "id_basin", "fec_production_day", "fec_update_date", "left")
    df = df.drop("id_basin").withColumnRenamed("id_vers", "id_basin")

    df = joinWithDim(df, dfDimOperRoute, Seq("fec_production_day", "id_well", "fec_update_date"), "cp_operator_route_id", "id_operator_route", "fec_production_day", "fec_start_date", "left")
    df = df.drop("cp_operator_route_id").withColumnRenamed("id_vers", "id_operator_route")

    df = joinWithDim(df, dfDimCollecPoint, Seq("fec_production_day", "id_well", "fec_update_date"), "cp_col_point_id", "id_collection_point", "fec_production_day", "fec_start_date", "left")
    df = df.drop("cp_col_point_id").withColumnRenamed("id_vers", "id_col_point")

    df = joinWithDim(df, dfDimLicence, Seq("fec_production_day", "id_well", "fec_update_date"), "mur_id", "id_licence", "fec_production_day", "fec_start_date", "left")
    df = df.drop("mur_id").withColumnRenamed("id_vers", "id_licence")

    df = selectFields(df)

    df = df.withColumn("__calculated_key__", concat_ws("_", auxParamsFinal.get("keys").map(toScalaList).getOrElse(Seq.empty[String]).map(col): _*))
    
    val (targetDf, targetDelta) = getDeltaTable(df, targetFinal)
    applyChange(targetFinal, df)
 
    optimizeDeltaFun(targetDelta, getObject(targetFinal("target_object").toString) ,auxParamsFinal("dayOfWeek").toString)

    dlMsg = "Rows Inserted: " + df.count
    dlInsert = "OK"

} catch {
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

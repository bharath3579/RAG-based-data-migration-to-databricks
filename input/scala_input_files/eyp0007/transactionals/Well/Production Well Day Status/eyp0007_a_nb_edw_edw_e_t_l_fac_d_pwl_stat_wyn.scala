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
"id_licence","fec_create_date","fec_update_date"
)

val selecAttributeSeq = Seq("ind_alloc_cond_vol_stb_wi","ind_alloc_cond_vol_stb_net","ind_alloc_cond_vol_sm3_wi","ind_alloc_cond_vol_sm3_net",
"ind_alloc_gas_vol_mscf_wi","ind_alloc_gas_vol_mscf_net","ind_alloc_gas_vol_kscf_wi","ind_alloc_gas_vol_kscf_net","ind_alloc_gas_vol_sm3_wi",
"ind_alloc_gas_vol_sm3_net","ind_alloc_net_oil_vol_stb_wi","ind_alloc_net_oil_vol_stb_net","ind_alloc_net_oil_vol_sm3_wi","ind_alloc_net_oil_vol_sm3_net",
"ind_o_alloc_cond_vol_stb_wi","ind_o_alloc_cond_vol_stb_net","ind_o_alloc_cond_vol_sm3_wi","ind_o_alloc_cond_vol_sm3_net",
"ind_o_alloc_gas_vol_mscf_wi","ind_o_alloc_gas_vol_mscf_net","ind_o_alloc_gas_vol_sm3_wi","ind_o_alloc_gas_vol_sm3_net",
"ind_o_alloc_gas_vol_kscf_wi","ind_o_alloc_gas_vol_kscf_net","ind_o_alloc_net_oil_vol_stb_wi","ind_o_alloc_net_oil_vol_stb_net",
"ind_o_alloc_net_oil_vol_sm3_wi","ind_o_alloc_net_oil_vol_sm3_net","ind_p_alloc_cond_vol_stb_wi","ind_p_alloc_cond_vol_stb_net",
"ind_p_alloc_cond_vol_sm3_wi","ind_p_alloc_cond_vol_sm3_net","ind_p_alloc_gas_vol_mscf_wi","ind_p_alloc_gas_vol_mscf_net",
"ind_p_alloc_gas_vol_sm3_wi","ind_p_alloc_gas_vol_sm3_net","ind_p_alloc_gas_vol_kscf_wi","ind_p_alloc_gas_vol_kscf_net",
"ind_p_alloc_net_oil_vol_stb_wi","ind_p_alloc_net_oil_vol_stb_net","ind_p_alloc_net_oil_vol_sm3_wi","ind_p_alloc_net_oil_vol_sm3_net")

def selectFields (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    val selectFields = selectFieldsSeq ++ selecAttributeSeq

    
    selecAttributeSeq.foreach { field =>
        if (!dfTrn.columns.contains(field)) {
            println("Field not exist:" + field)
            dfTrn = dfTrn.withColumn(field, lit(null).cast("double"))
        }
    }

    dfTrn = dfTrn.select(selectFields.map(col): _*)
   
    return dfTrn

}


def joinDf(df1: DataFrame, df2: DataFrame, joinConditions: Map[String, String], selectFields: Seq[String], joinType: String): DataFrame = {

    val joinedDF = df1.join(df2, joinConditions.map { case (col1, col2) => df1(col1) === df2(col2) }.reduce(_ && _), joinType)
    val selectedDF = joinedDF.select((df1.columns.map(df1(_)) ++ selectFields.map(df2(_))):_*)
    selectedDF

}

def joinWithEqShare(df1: DataFrame, df2: DataFrame, fieldsPartition: Seq[String], selectFields: Seq[String], joinType: String): DataFrame = {
    
      // Construimos una especificación de ventana que toma los campos de partición de df1 o df2, según correspondan
    val partitionColumns = fieldsPartition.map(field => 
        if (df1.columns.contains(field)) df1(field)
        else if (df2.columns.contains(field)) df2(field)
        else throw new IllegalArgumentException(s"Field $field not found in either df1 or df2")
    )

    val windowSpec = Window.partitionBy(partitionColumns: _*).orderBy(df2("fec_start_date").desc)
    
    val joinedDF = df1.join(df2, df1("id_commercial_entity") === df2("id_commercial_entity") && df1("des_fluid_type") === df2("des_fluid_type") 
                                            && df2("fec_production_day") === df1("fec_production_day"), joinType)
    val windowDF = joinedDF.withColumn("row_number",row_number().over(windowSpec)).filter("row_number == 1")

    val selectedDF = windowDF.select((df1.columns.map(df1(_)) ++ selectFields.map(df2(_))):_*)
    selectedDF

}

def toTranspose(df: DataFrame, columnsFixed: Seq[String], columnsTransposed: Seq[String]): DataFrame = {
  val transposeDf = df.select(
    (columnsFixed.map(col) ++
    Seq(expr(s"stack(${columnsTransposed.length}," + 
      columnsTransposed.map(colName => s"'$colName', $colName").mkString(", ") + ") as (attribute, value)"))): _*
  ).select((columnsFixed.map(col) ++ Seq(upper(col("attribute")), col("value"))): _*).withColumnRenamed("upper(attribute)", "attribute")
  transposeDf
}


def toPivot(df: DataFrame, columnsFixed: Seq[String]): DataFrame = {
  val pivotedDF = df.groupBy(columnsFixed.map(col): _*)
  .pivot(when(col("attribute_eq_share").isNotNull, lower(concat($"attribute", lit("_"), $"attribute_eq_share"))).otherwise(lower($"attribute")))
  .agg(first("value_final", ignoreNulls = true))
  pivotedDF
}


// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------


var dlInsert = "N/A"
var dlMsg = ""

val flagFullLoad = paramsFinal.getOrElse("full_load", "false").toString.toBoolean

var df: DataFrame  = null
var dfTransactional: DataFrame = null 

if (!flagFullLoad){
            val fieldsFilter = auxParamsFinal.getOrElse("fieldsFilter", List("fec_create_date", "fec_update_date")).asInstanceOf[List[String]]

            var referenceDateAux = if (referenceDate!= null) {referenceDate} else {""}
            if (referenceDateAux.isEmpty){
                referenceDateAux = returnDateNowString()
            }

            val filterWhere = getWhereFilterDays((referenceDateAux), fieldsFilter, 5)
            dfTransactional = getDataFromSource(sourcesFinal("am_eyp0007_tb_fac_d_pwel_day_all_full"), filterWhere)
}else {
            dfTransactional = getDataFromSource(sourcesFinal("am_eyp0007_tb_fac_d_pwel_day_all_full"))
        }



val dfFacEqShare = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_eq_share"))
val dfDimCompany = getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_company"))



try {


    val dfJoinEqShareCompany = joinDf(dfFacEqShare, dfDimCompany.filter("lower(des_company) LIKE '%repsol%'"), Map("id_company" -> "id_vers"),
                    Seq("des_company"), "inner")
                    .withColumnRenamed("ind_eco_share", "wi")
                    .withColumnRenamed("ind_net_part", "net")

    val fieldsFixedEqShare = Seq("fec_production_day","id_commercial_entity", "fec_start_date", "fec_end_date", "des_fluid_type", "id_company", 
                    "fec_create_date", "fec_update_date")
    val fieldsTransposeEqShare = Seq("wi", "net")
    val dfEqShareTranspose = toTranspose(dfJoinEqShareCompany, fieldsFixedEqShare, fieldsTransposeEqShare)
                    .withColumnRenamed("attribute", "attribute_eq_share")
                    .withColumnRenamed("value", "value_eq_share")

    df = dfTransactional

    val fieldsFixedTransactional = Seq("fec_production_day", "id_vers_well", "id_well", "id_commercial_entity",
    "id_facility_class_1", "id_well_hookup", "id_well_hole", "id_area", "id_productionunit", "id_geo_area", "id_field_cds", 
    "id_field", "id_basin", "id_operator_route", "id_col_point", "id_licence", "fec_create_date", "fec_update_date")
    val fieldsTranposeTransactional = Seq("ind_alloc_cond_vol_stb","ind_alloc_cond_vol_sm3","ind_alloc_gas_vol_mscf","ind_alloc_gas_vol_kscf","ind_alloc_gas_vol_sm3",
    "ind_alloc_net_oil_vol_stb","ind_alloc_net_oil_vol_sm3","ind_o_alloc_cond_vol_stb","ind_o_alloc_cond_vol_sm3","ind_o_alloc_gas_vol_mscf",
    "ind_o_alloc_gas_vol_sm3","ind_o_alloc_gas_vol_kscf","ind_o_alloc_net_oil_vol_stb","ind_o_alloc_net_oil_vol_sm3","ind_p_alloc_cond_vol_stb",
    "ind_p_alloc_cond_vol_sm3","ind_p_alloc_gas_vol_mscf","ind_p_alloc_gas_vol_sm3","ind_p_alloc_gas_vol_kscf","ind_p_alloc_net_oil_vol_stb",
    "ind_p_alloc_net_oil_vol_sm3")


    fieldsTranposeTransactional.foreach { field =>
            if (df.columns.contains(field)) {
                df = df.withColumn(field, col(field).cast("double"))
            }
    }


    df = toTranspose(df, fieldsFixedTransactional,fieldsTranposeTransactional)
                .withColumn("des_fluid_type", when(lower(col("attribute")).contains("_cond_"), "COND")
                                                .when(lower(col("attribute")).contains("_oil_"), "OIL")
                                                .when(lower(col("attribute")).contains("_gas_"), "GAS")
                                                .otherwise(lit(null)))

val fieldsPartition: Seq[String] = fieldsFixedTransactional :+ "attribute" :+ "attribute_eq_share" //Se incluyen estos campos en la particion para no eliminar nigun registro

    df = joinWithEqShare(df, dfEqShareTranspose, fieldsPartition, Seq("attribute_eq_share", "value_eq_share"), "left")
  
    df = df.withColumn("value_final", (col("value") * (col("value_eq_share")/100)))

    df = toPivot(df, fieldsFixedTransactional)

    df = selectFields(df)

    df = df.withColumn("__calculated_key__", concat_ws("_", auxParamsFinal.get("keys").map(toScalaList).getOrElse(Seq.empty[String]).map(col): _*))

    if (flagFullLoad) {
        writeDataFrame(df, targetFinal)
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

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

import org.apache.spark.sql.functions._
val order = Seq("id_vers", "id_well_hole", "des_well_hole", "cod_uwi", "des_well_name", "cod_api10","fec_start_date", "fec_end_date", "ind_bh_latitude", "ind_bh_longitude", 
"des_country", "ind_latitude", "ind_longitude", "id_fcty_1", "des_status", "id_basin", "des_common_name", "des_county", "des_country_name", "des_ground_elevation", 
"des_ground_elevation_unit", "des_normalized_geographic_coordinate_system", "des_onshore_or_offshore", "des_slot_name", "fec_spud_date", "des_state_or_province", 
 "val_normalized_latitud", "val_normalized_longitude", "des_current_operator", "nom_pref_proj_coord_sys", "val_preferred_easting","val_preferred_northing",
 "nom_pref_geogr_coord_sys","val_preferred_latitude" ,"val_preferred_longitude","des_existence_kind","des_installation_name","val_water_depth","val_datum_elevation", "des_datum_name"
 ,"fec_completed_start","fec_completed_end","fec_create_date", "fec_update_date", "fec_update_cds")


 val rowDummy = Row("-99", "-99", null, "-99", null, "-99",java.sql.Timestamp.valueOf("1900-01-01 00:00:00"),java.sql.Timestamp.valueOf("1900-01-01 00:00:00"),
null, null, null, null, null, null, null,  
null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
null, null, null, null, null, null, null, null, null, null, null, null) 


def addRowDummy(df: DataFrame): DataFrame = {
    
    var dfTrn: DataFrame = df

    val dfDummy = spark.createDataFrame(Seq(rowDummy).asJava, df.schema)

    dfTrn = dfTrn.union(dfDummy)

    return dfTrn

}


def transformationDfWell (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("basin_identifier", "id_basin")
                    .withColumnRenamed("common_name", "des_common_name")
                    .withColumnRenamed("county", "des_county")
                    .withColumnRenamed("country_name", "des_country_name")
                    .withColumnRenamed("ground_elevation", "des_ground_elevation")
                    .withColumnRenamed("ground_elevation_unit", "des_ground_elevation_unit")
                    .withColumnRenamed("normalized_geographic_coordinate_system", "des_normalized_geographic_coordinate_system")
                    .withColumnRenamed("onshore_or_offshore", "des_onshore_or_offshore")
                    .withColumnRenamed("slot_name", "des_slot_name")
                    .withColumnRenamed("spud_date", "fec_spud_date")
                    .withColumnRenamed("state_or_province", "des_state_or_province")
                    .withColumnRenamed("well_name", "des_well_name")
                    .withColumnRenamed("normalized_latitude", "val_normalized_latitud").withColumn("val_normalized_latitud", col("val_normalized_latitud").cast("Double"))
                    .withColumnRenamed("normalized_longitude", "val_normalized_longitude").withColumn("val_normalized_longitude", col("val_normalized_longitude").cast("Double"))
                    .withColumnRenamed("unique_well_identifier", "cod_uwi")
                    .withColumnRenamed("update_date", "fec_update_cds")
                    .withColumnRenamed("current_operator", "des_current_operator")
                    .withColumnRenamed("preferred_projected_coordinate_system", "nom_pref_proj_coord_sys")
                    .withColumnRenamed("preferred_easting", "val_preferred_easting").withColumn("val_preferred_easting", col("val_preferred_easting").cast("Double"))
                    .withColumnRenamed("preferred_northing", "val_preferred_northing").withColumn("val_preferred_northing", col("val_preferred_northing").cast("Double"))
                    .withColumnRenamed("preferred_geographic_coordinate_system", "nom_pref_geogr_coord_sys")
                    .withColumnRenamed("preferred_latitude", "val_preferred_latitude").withColumn("val_preferred_latitude", col("val_preferred_latitude").cast("Double"))
                    .withColumnRenamed("preferred_longitude", "val_preferred_longitude").withColumn("val_preferred_longitude", col("val_preferred_longitude").cast("Double"))
                    .withColumnRenamed("existence_kind", "des_existence_kind")
                    .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))

   
    return dfTrn

}

//Filtramos installation por PAD y renombramos para join y schema
def transformationDfInst (df: DataFrame): DataFrame = {

    var dfTrn = df

    dfTrn = dfTrn.filter(col("installation_type") === "PAD")
                .select("unique_well_identifier", "installation_name", "start_date")
                .withColumnRenamed("unique_well_identifier", "cod_uwi")
                .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
                .withColumnRenamed("installation_name", "des_installation_name")
                .withColumnRenamed("start_date", "fec_start_date_inst")
    
    return dfTrn
}

//Filtramos well alias para obtener el api10 y renombramos para join y schema
def transformationDfWellAlias (df: DataFrame): DataFrame = {

    var dfTrn = df

    dfTrn = dfTrn.filter(col("well_name_set") === "government identifier")
                .withColumn("alias_fix", regexp_replace(col("alias"), "-", ""))
                .select("unique_well_identifier", "alias_fix", "effective_date")
                .withColumnRenamed("unique_well_identifier", "cod_uwi")
                .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
                .withColumnRenamed("alias_fix", "cod_api10").withColumn("cod_api10", substring(regexp_replace(col("cod_api10"), "[^0-9]", ""),1, 10))
                .withColumnRenamed("effective_date", "fec_start_date_alias")
    
    return dfTrn
}

def transformationDfWellEdms (df: DataFrame): DataFrame = {

    var dfTrn = df

    dfTrn = dfTrn.select("REPSOL_WELL_UWI", "WATER_DEPTH", "UPDATE_DATE", "WELL_ID")
                .withColumnRenamed("REPSOL_WELL_UWI", "cod_uwi")
                .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
                .withColumnRenamed("WATER_DEPTH", "val_water_depth")
                .withColumnRenamed("UPDATE_DATE", "update_date_edm_well")
                .withColumnRenamed("WELL_ID", "join_edm")
                .withColumn("join_edm", trim(regexp_replace(col("join_edm"), "\\s+", "")))
    
    return dfTrn
}

def transformationDfDatum (df: DataFrame): DataFrame = {

    var dfTrn = df

    dfTrn = dfTrn.filter(col("IS_DEFAULT") === "Y")
                .select("WELL_ID", "DATUM_ELEVATION", "DATUM_NAME","UPDATE_DATE")
                .withColumnRenamed("WELL_ID", "join_edm")
                .withColumnRenamed("DATUM_ELEVATION", "val_datum_elevation")
                .withColumnRenamed("DATUM_NAME", "des_datum_name")
                .withColumnRenamed("UPDATE_DATE", "update_date_edm_datum")
                .withColumn("join_edm", trim(regexp_replace(col("join_edm"), "\\s+", "")))
    
    return dfTrn
}


def transformationDfEvent (df: DataFrame) : DataFrame = {

    var dfTrn = df

    dfTrn = dfTrn.filter(col("Event_Type").isin("COMPLETION", "COMPLETION (HIST EQUINOR)", "UNCONVENTIONAL COMPLETION"))
                .select("WELL_ID", "DATE_OPS_START", "DATE_OPS_END")
                .withColumnRenamed("WELL_ID", "join_edm")
                .withColumnRenamed("DATE_OPS_START", "fec_completed_start")
                .withColumnRenamed("DATE_OPS_END", "fec_completed_end")
                .orderBy(col("join_edm"), desc("fec_completed_start"))
                .dropDuplicates("join_edm")
                .withColumn("join_edm", trim(regexp_replace(col("join_edm"), "\\s+", "")))
    
    return dfTrn
}


//Join entre well_hole EC y Well CDS, el join será right para que mande CDS. Se genera una nueva columna id_vers y fecha de inicio para corregir registros de CDS
def joinWellandFixIdVers(dfL: DataFrame, dfR: DataFrame, keyCol: String, joinType: String): DataFrame = {

    var resDf = dfL.join(dfR, Seq(keyCol), joinType)

    resDf = resDf.withColumn("fec_start_date", to_timestamp(coalesce(col("fec_start_date"), lit("1970-01-01"))))
                 .withColumn("fix_id_vers", when(col("id_well_hole").isNull, 
                 concat(date_format(col("fec_start_date"), "yyyyMMdd"), col(keyCol)))
                 .otherwise(concat(date_format(col("fec_start_date"), "yyyyMMdd"), col("id_well_hole"), col(keyCol))))
                 .drop("id_vers")
                 .withColumnRenamed("fix_id_vers", "id_vers")
                 .withColumn("id_well_hole", coalesce(col("id_well_hole"), lit("-99")))

    return resDf
   
}
//Join general para el resto de maestros
def joinWell (dfL: DataFrame, dfR: DataFrame, orderCol: String, keyCol: String): DataFrame = {

    val hWin = Window.partitionBy(col("id_vers"), col("fec_start_date")).orderBy(col(orderCol).desc)

    var resDf = dfL.join(dfR, Seq(keyCol), "left")

    return resDf.withColumn("new", row_number().over(hWin) ).where(col("new") === 1)
}

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

def addBusinessUnitColumn(df: DataFrame): DataFrame = {
  // Definimos la lógica de asignación con la función when de Spark
  val businessUnitColumn = when(col("des_state_or_province").isin("PENNSYLVANIA", "NEW YORK"), "Marcellus")
    .when(col("des_state_or_province").equalTo("TEXAS"), "EagleFord")
    .otherwise(null)

  // Agregamos la nueva columna al DataFrame
  df.withColumn("des_business_unit", businessUnitColumn)
}

def addDesDevArea(df: DataFrame): DataFrame = {
  df.withColumn(
    "des_dev_area",
    when(col("des_business_unit") === "Marcellus", 
      when(col("des_installation_name").contains("(01-"), "TROY")
      .when(col("des_installation_name").contains("(02-"), "STATE LANDS")
      .when(col("des_installation_name").contains("(03-"), "COLUMBIA")
      .when(col("des_installation_name").contains("(04-"), "JACKSON")
      .when(col("des_installation_name").contains("(05-"), "CHAFFEE")
      .when(col("des_installation_name").contains("(07-"), "FRIENDSVILLE")
      .when(col("des_installation_name").contains("(08-"), "TEXAS CREEK")
      .when(col("des_installation_name") === "DELCIOTTO 1", "TEXAS CREEK")
      .when(col("des_installation_name") === "DELCIOTTO 2", "TEXAS CREEK")
      .when(col("des_installation_name") === "GRAHAM 2", "TEXAS CREEK")
      .when(!col("des_installation_name").contains("(01-"), "TRENTON BLACK RIVER")
      .otherwise("NULL")
    ).otherwise(lit(null)) // Si no es "Marcellus", deja NULL
  )
}


def mergehrhy(df1: DataFrame, df2: DataFrame): DataFrame = {

  //val df22  = df2.withColumn("des_area", when(col("des_area") === "No Available", lit(null)).otherwise(col("des_area")))

  // Seleccionamos solo las columnas necesarias de df2
  val dfsel = df2.select("id_final", "des_area")
  
  // Realizamos el LEFT JOIN con df1:id_vers y df2:id_final
  val dfjoins = df1.join(dfsel, df1("id_vers") === dfsel("id_final"), "left_outer")
                   .drop("id_final") // Eliminamos id_final si no lo necesitamos
  val dfWithDesDevArea = dfjoins.withColumn(
    "des_dev_area",
    when(col("des_business_unit") === "EagleFord", col("des_area"))
      .otherwise(col("des_dev_area")) // Si no es EagleFord, mantenemos el valor original de des_dev_area
  )

  val dfFinal = dfWithDesDevArea.withColumn(
    "des_dev_area", 
    when(col("des_dev_area") === "Not available", lit(null))
      .otherwise(col("des_dev_area"))
  )

  dfFinal.drop(col("des_area"))
}


// COMMAND ----------

/**
 * Para cada valor distinto de `idCol`, conserva únicamente la fila cuya `dateCol` sea máxima.
 *
 * @param df      DataFrame de entrada.
 * @param idCol   Nombre de la columna de ID (en tu caso: "id_vers").
 * @param dateCol Nombre de la columna de fecha (en tu caso: "fec_start_date_rel").
 *                Si viene como String, internamente la convierte a Timestamp; 
 *                si ya es Date/Timestamp, la deja igual.
 * @return DataFrame filtrado sin duplicados de idCol, quedándote sólo con la fila de fecha más reciente.
 */
def keepMostRecentById(df: DataFrame, idCol: String, dateCol: String): DataFrame = {
  // 1) Si dateCol viene como StringType, conviértela a TimestampType (ajusta el formato si tus fechas no son "yyyy-MM-dd HH:mm:ss").
  val df2 = df.schema(dateCol).dataType.typeName match {
    case "string" =>
      df.withColumn(dateCol, to_timestamp(col(dateCol), "yyyy-MM-dd HH:mm:ss"))
    case _ =>
      df
  }

  // 2) Definimos la ventana: particionada por idCol, ordenada descendentemente por dateCol
  val windowSpec = Window
    .partitionBy(col(idCol))
    .orderBy(col(dateCol).desc)

  // 3) Asignamos row_number() dentro de cada partición; mantenemos sólo rn == 1
  df2
    .withColumn("rn", row_number().over(windowSpec))
    .filter(col("rn") === 1)
    .drop("rn")
}


// COMMAND ----------

var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {
        val dfWellHole: DataFrame = getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_dim_d_well_hole")).withColumnRenamed("id_cds", "cod_uwi")
                                                                            .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
                                                                            .withColumn("ind_bh_latitude", col("ind_bh_latitude").cast("Double"))
                                                                            .withColumn("ind_bh_longitude", col("ind_bh_longitude").cast("Double"))
                                                                            .withColumn("ind_latitude", col("ind_latitude").cast("Double"))
                                                                            .withColumn("ind_longitude", col("ind_longitude").cast("Double"))

        val dfWellCds: DataFrame = transformationDfWell(getDataFromSource(sourcesFinal("aa001163_glb_cds_well")))
        val dfInst = transformationDfInst(getDataFromSource(sourcesFinal("aa001163_glb_cds_inst_well_connection")))
        val dfWellAlias = transformationDfWellAlias(getDataFromSource(sourcesFinal("aa001163_glb_cds_well_alias")))
        val dfWellEdm = transformationDfWellEdms(getDataFromSource(sourcesFinal("aa001236_glb_cd_well_t")))
        val dfDatum = transformationDfDatum(getDataFromSource(sourcesFinal("aa001236_glb_cd_datum_t")))
        val dfDEvent = transformationDfEvent(getDataFromSource(sourcesFinal("aa001236_glb_dm_event_t")))
        
        
        // dfWellHole(master_sys_code) - left - dfWell(unique_well_identifier)
        df = joinWellandFixIdVers(dfWellHole, dfWellCds, "cod_uwi", "right")

        df = joinWell(df, dfInst, "fec_start_date_inst", "cod_uwi")


        df = joinWell(df, dfWellAlias, "fec_start_date_alias", "cod_uwi") 
 
        df = joinWell(df, dfWellEdm, "update_date_edm_well", "cod_uwi") 

        df = joinWell(df, dfDatum, "update_date_edm_datum", "join_edm")

        df = joinWell(df, dfDEvent, "fec_update_cds", "join_edm")

        df = df.select(order.map(col): _*)

        df = addRowDummy(df)
        df = addBusinessUnitColumn(df)
        df = addDesDevArea(df)

        df = mergehrhy(df,getDataFromSource(sourcesFinal("eyp0007_tb_aux_d_dim_hrchy")))
        val totalFilas = df.count()
        df = keepMostRecentById(df, "id_well_hole", "fec_start_date")
        
        

// 2. Conteo de valores distintos en “id_vers”
val totalDistinct = df.select("id_vers").distinct().count()

println(s"Total de filas = $totalFilas")
println(s"Total de id_vers distintos = $totalDistinct")

    val dfFormatted = applyDDLFromTableToDF(df,getObject((target_object).toString))
 
    writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

    println("Se han insertado "+df.count+" registros")

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

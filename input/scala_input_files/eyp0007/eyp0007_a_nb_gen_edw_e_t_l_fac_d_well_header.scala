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
var error:String = ""

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

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

val orderFields: Seq[String] = Seq("cod_uwi", "des_common_name", "des_well_name", "des_well_alias","des_current_operator", 
"des_country_name", "des_state_or_province", "des_county", "des_environment","id_basin", "des_short_name", "des_existence_kind", 
"fec_spud_date", "fec_abandoment_date", "val_well_net_interest", "des_well_net_interest_unit", "val_water_depth", 
"des_water_depth_unit", "val_ground_elevation", "des_ground_elevation_unit", "des_slot_name", "des_description", 
"des_data_source", "fec_create_date", "des_create_user_id", "fec_update_date", "des_update_user_id", "des_installation_name", 
"des_installation_type", "des_installation_identifier", "des_gov_identifier", "des_proj_coord_unit", "des_proj_coord_sys", 
"val_easting", "val_northing", "des_geo_coord_sys", "val_latitude", "val_longitude", "des_well_ownership", "des_well_business_unit", 
"des_well_business_intention", "des_well_category", "des_posc_well_fluid_type", "des_elevation_type", "val_elevation", "des_elevation_unit", 
"des_elevation_datum")


def transformationDfWell(df: DataFrame): DataFrame = {

  val dfTrn = df
    .withColumnRenamed("unique_well_identifier", "cod_uwi")
    .withColumnRenamed("common_name", "des_common_name")
    .withColumnRenamed("well_name", "des_well_name")
    .withColumnRenamed("current_operator", "des_current_operator")
    .withColumnRenamed("country_name", "des_country_name")
    .withColumnRenamed("state_or_province", "des_state_or_province")
    .withColumnRenamed("county", "des_county")
    .withColumnRenamed("onshore_or_offshore", "des_environment")
    .withColumnRenamed("basin_identifier", "id_basin")
    .withColumnRenamed("plot_name", "des_short_name")
    .withColumnRenamed("existence_kind", "des_existence_kind")
    .withColumnRenamed("spud_date", "fec_spud_date")
    .withColumn("fec_spud_date", to_timestamp(col("fec_spud_date")))
    .withColumnRenamed("abandonment_date", "fec_abandoment_date")
    .withColumn("fec_abandoment_date", to_timestamp(col("fec_abandoment_date")))
    .withColumnRenamed("well_net_interest", "val_well_net_interest").withColumn("val_well_net_interest", col("val_well_net_interest").cast("double"))
    .withColumnRenamed("well_net_interest_unit", "des_well_net_interest_unit")
    .withColumnRenamed("water_depth", "val_water_depth").withColumn("val_water_depth", col("val_water_depth").cast("double"))
    .withColumnRenamed("water_depth_unit", "des_water_depth_unit")
    .withColumnRenamed("ground_elevation", "val_ground_elevation")
    .withColumn("val_ground_elevation", col("val_ground_elevation").cast("double"))
    .withColumnRenamed("ground_elevation_unit", "des_ground_elevation_unit")
    .withColumnRenamed("slot_name", "des_slot_name")
    .withColumnRenamed("description", "des_description")
    .withColumnRenamed("data_source", "des_data_source")
    .withColumnRenamed("create_date", "fec_create_date")
    .withColumnRenamed("create_user_id", "des_create_user_id")
    .withColumnRenamed("update_date", "fec_update_date")
    .withColumnRenamed("update_user_id", "des_update_user_id")
    .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
    .select(
      "cod_uwi",
      "des_common_name",
      "des_well_name",
      "des_current_operator",
      "des_country_name",
      "des_state_or_province",
      "des_county",
      "des_environment",
      "id_basin",
      "des_short_name",
      "des_existence_kind",
      "fec_spud_date",
      "fec_abandoment_date",
      "val_well_net_interest",
      "des_well_net_interest_unit",
      "val_water_depth",
      "des_water_depth_unit",
      "val_ground_elevation",
      "des_ground_elevation_unit",
      "des_slot_name",
      "des_description",
      "des_data_source",
      "fec_create_date",
      "des_create_user_id",
      "fec_update_date",
      "des_update_user_id"
    )

  dfTrn
}


def transformationDfInst(df: DataFrame): DataFrame = {

  val dfTrn = df
    .filter(col("installation_type") === "PAD")
    .withColumn("cod_uwi", trim(regexp_replace(col("unique_well_identifier"), "\\s+", "")))
    .withColumnRenamed("installation_name", "des_installation_name")
    .withColumnRenamed("installation_type", "des_installation_type")
    .withColumnRenamed("installation_identifier", "des_installation_identifier")
    .select(
      "cod_uwi",
      "des_installation_name",
      "des_installation_type",
      "des_installation_identifier"
    )

  dfTrn
}


//Filtramos well alias para obtener el api10 y renombramos para join y schema
def transformationDfWellAlias(df: DataFrame): DataFrame = {
  val dfTrn = df
    .filter(col("well_name_set").isin("government identifier", "generic alias"))
    .withColumn(
      "des_gov_identifier",
      when(col("well_name_set") === "government identifier",
        regexp_replace(col("alias"), "[^A-Za-z0-9]", "")
      )
    )
    .withColumn(
      "des_well_alias",
      when(col("well_name_set") === "generic alias", col("alias"))
    )
    .withColumn("cod_uwi", trim(regexp_replace(col("unique_well_identifier"), "\\s+", "")))
    .select("cod_uwi", "des_gov_identifier", "des_well_alias")

  dfTrn
}


def transformationWellSurfLoc(df: DataFrame): DataFrame = {

    var dfTrn = df

    dfTrn = dfTrn.withColumnRenamed("unique_well_identifier", "cod_uwi")
                .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
                .withColumnRenamed("effective_date", "fec_start_date_alias").withColumn("fec_start_date_alias", col("fec_start_date_alias").cast(StringType))
                .drop("well_name")
                .drop("description")
                .drop("create_date")
                .drop("create_user_id")
                .drop("update_date")
                .drop("update_user_id")
                .drop("data_source")
    
    return dfTrn
}

def transformationWellClass(df: DataFrame): DataFrame = {
  // 1) Renombrado y limpieza de la PK
  val dfTrn = df
    .withColumnRenamed("unique_well_identifier", "cod_uwi")
    .withColumn("cod_uwi", trim(regexp_replace(col("cod_uwi"), "\\s+", "")))
    .drop("cod_api10")
    .drop("well_name")
    .drop("description")
    .drop("create_date")
    .drop("create_user_id")
    .drop("update_date")
    .drop("update_user_id")

  // 2) Lista de valores de class_kind a pivotear
  val classKinds = Seq(
    "well business unit",
    "well ownership",
    "well category",
    "well business intention",
    "POSC well fluid type"
  )

  // 3) Columnas que queremos agrupar (todas menos class_kind y class)
  val colsToGroup = dfTrn.columns.filterNot(c => Seq("class_kind", "class").contains(c))

  // 4) Pivot
  val dfPivoted = dfTrn
    .groupBy(colsToGroup.map(col): _*)
    .pivot("class_kind", classKinds)
    .agg(first(col("class")))

  // 5) Renombrado de columnas pivotadas con prefijo des_
  val renamedColsMap = classKinds.map { kind =>
    kind -> s"des_${kind.replaceAll("\\s+", "_").toLowerCase}"
  }.toMap

  val dfRenamed = renamedColsMap.foldLeft(dfPivoted) { case (dfAcc, (original, renamed)) =>
    dfAcc.withColumnRenamed(original, renamed)
  }

  // 6) Selección final sin usar :_*
  val selectedCols = Seq("cod_uwi") ++ renamedColsMap.values.toSeq
  dfRenamed.select(selectedCols.map(col): _*)
}




//Join general para el resto de maestros
def joinWell (dfL: DataFrame, dfR: DataFrame, orderCol: String, keyCol: String): DataFrame = {

    val hWin = Window.partitionBy(col("id_vers"), col("fec_start_date")).orderBy(col(orderCol).desc)

    var resDf = dfL.join(dfR, Seq(keyCol), "left")

    return resDf.withColumn("new", row_number().over(hWin) ).where(col("new") === 1)
}

def transformDfLocation(df: DataFrame): DataFrame = {
  df
    .withColumnRenamed("unique_well_identifier", "cod_uwi")
    .withColumnRenamed("projected_coordinate_unit", "des_proj_coord_unit")
    .withColumnRenamed("projected_coordinate_system", "des_proj_coord_sys")
    .withColumnRenamed("easting", "val_easting").withColumn("val_easting", col("val_easting").cast("double"))
    .withColumnRenamed("northing", "val_northing").withColumn("val_northing", col("val_northing").cast("double"))
    .withColumnRenamed("geographic_coordinate_system", "des_geo_coord_sys")
    .withColumnRenamed("latitude", "val_latitude").withColumn("val_latitude", col("val_latitude").cast("double"))
    .withColumnRenamed("longitude", "val_longitude").withColumn("val_longitude", col("val_longitude").cast("double"))
    .select(
      "cod_uwi",
      "des_proj_coord_unit",
      "des_proj_coord_sys",
      "val_easting",
      "val_northing",
      "des_geo_coord_sys",
      "val_latitude",
      "val_longitude"
    )
}

def transformationDfRefPoint(df: DataFrame): DataFrame = {

  df
    .withColumnRenamed("unique_well_identifier", "cod_uwi")
    .withColumnRenamed("depth_reference_point", "des_elevation_type")
    .withColumnRenamed("depth_reference_elevation", "val_elevation").withColumn("val_elevation", col("val_elevation").cast("double"))
    .withColumnRenamed("depth_reference_elevation_unit", "des_elevation_unit")
    .withColumnRenamed("depth_reference_datum", "des_elevation_datum")
    .select(
      "cod_uwi",
      "des_elevation_type",
      "val_elevation",
      "des_elevation_unit",
      "des_elevation_datum"
    )
}

def assembleWellClass(df: DataFrame): DataFrame = {
  // 1) Lista de tus columnas de clase (ajústala a tu esquema real)
  val classCols = Seq(
    "des_well_business_unit",
    "des_well_business_intention",
    "des_well_category",
    "des_posc_well_fluid_type"
    // si tienes más, por ejemplo:
    // , "well_category"
    // , "posc_well_fluid_type"
  )

  // 2) Columnas “estáticas”: todas menos cod_uwi y las de classCols
  val otherCols = df.columns.filterNot(c => c == "cod_uwi" || classCols.contains(c))

  // 3) Para cada columna “estática”: primer valor no-null/no-"undefined"
  val otherAggs = otherCols.map { c =>
    first(
      when(col(c).isNull   || col(c) === "undefined", lit(null))
        .otherwise(col(c)),
      ignoreNulls = true
    ).as(c)
  }

  // 4) Para cada columna de clase: igual, primer valor válido
  val classAggs = classCols.map { c =>
    first(
      when(col(c).isNull   || col(c) === "undefined", lit(null))
        .otherwise(col(c)),
      ignoreNulls = true
    ).as(c)
  }

  // 5) Agrupamos TODO en un solo groupBy/agg
  val allAggs = otherAggs ++ classAggs
  df.groupBy("cod_uwi")
    .agg(allAggs.head, allAggs.tail: _*)
}

// COMMAND ----------

def renameDf(df: DataFrame): DataFrame = {
  df
    .withColumnRenamed("well_business_intention", "des_well_business_intention")
    .withColumnRenamed("well_ownership",          "des_well_ownership")
    .withColumnRenamed("plot_name",          "des_plot_name")
    .withColumnRenamed("well_number", "num_well").withColumn("num_well", col("num_well").cast(IntegerType))
    .withColumnRenamed("identifier_name_set",          "cod_name_set")
    .withColumnRenamed("governmental_area",          "des_gov_area")
    .withColumnRenamed("abandonment_date",          "fec_abandonment_date")
    .withColumnRenamed("basin_name",          "des_basin_name")
    .withColumnRenamed("basin_abbreviation",          "des_basin_abbreviation")
    .withColumnRenamed("basin_type",          "des_basin_type")
    .withColumnRenamed("platform_code",          "cod_platform")
    .withColumnRenamed("survey_location_type",          "des_survey_location_type")
    .withColumnRenamed("location_status",          "des_location_status")
    .withColumnRenamed("location_remark",          "des_location_remark")
    .withColumnRenamed("water_depth",          "val_water_depth").withColumn("val_water_depth", col("val_water_depth").cast("Double"))
    .withColumnRenamed("water_depth_unit",          "des_water_depth_unit")
    .withColumnRenamed("well_net_interest",          "val_well_net_interest").withColumn("val_well_net_interest", col("val_well_net_interest").cast("Double"))
    .withColumnRenamed("well_net_interest_unit",          "des_well_net_interest_unit")
    .withColumnRenamed("description",          "des_description")
    .withColumnRenamed("create_date",          "fec_create")
    .withColumnRenamed("create_user_id",          "fec_create_user_id")
    .withColumnRenamed("update_user_id",          "fec_update_user_id")
    .withColumnRenamed("start_date",          "fec_start")
    .withColumnRenamed("end_date",          "fec_end")
    .withColumnRenamed("data_source",          "des_data_source")
    .withColumnRenamed("well_business_unit",          "des_well_business_unit") 
    .withColumnRenamed("well_category",          "des_well_category")
    .withColumnRenamed("posc_well_fluid_type",          "des_posc_well_fluid_type")
    .withColumnRenamed("preferred_projected_coordinate_unit",          "des_preferred_projected_coordinate_unit")

}

// COMMAND ----------

def countDuplicates(df: DataFrame): Long = {
  val totalRows = df.count()
  val distinctRows = df.distinct().count()
  val duplicates = totalRows - distinctRows
  duplicates
}

def getDuplicateRows(df: DataFrame, cols: Seq[String]): DataFrame = {
  val duplicatesKeysDf = df
    .groupBy(cols.map(col): _*)
    .count()
    .filter(col("count") > 1)
    .drop("count")

  val duplicatesDf = df.join(duplicatesKeysDf, cols, "inner")
  duplicatesDf
}

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.sql.Timestamp

def nullifyOutOfRangeDates(df: DataFrame): DataFrame = {
  val lowerBound = Timestamp.valueOf("1753-01-01 00:00:00")
  val upperBound = Timestamp.valueOf("9999-12-31 23:59:59")

  // 📝 Lista de columnas a limpiar (puedes modificarla aquí)
  val targetCols = Seq("fec_spud_date", "fec_abandoment_date", "fec_create_date", "fec_update_date")


  var cleanedDF = df

  targetCols.foreach { colName =>
    if (df.columns.contains(colName)) {
      val outOfRangeCount = df
        .filter(col(colName).isNotNull)
        .filter(col(colName) < lit(lowerBound) || col(colName) > lit(upperBound))
        .count()

      if (outOfRangeCount > 0) {
        
        cleanedDF = cleanedDF.withColumn(
          colName,
          when(col(colName).between(lowerBound, upperBound), col(colName)).otherwise(lit(null))
        )
      }
    } else {

    }
  }

  cleanedDF
}


// COMMAND ----------

var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {   

        val dfWell = transformationDfWell(getDataFromSource(sourcesFinal("aa001163_glb_cds_well")))
        
        val dfInstWellConn = transformationDfInst(getDataFromSource(sourcesFinal("aa001163_glb_cds_inst_well_connection")))

        val dfWellAlias = transformationDfWellAlias(getDataFromSource(sourcesFinal("aa001163_glb_cds_well_alias"))) 
        
        val dfWellSurfLoc = transformDfLocation(getDataFromSource(sourcesFinal("aa001163_glb_cds_well_surface_location")))

        val dfWellClass = transformationWellClass(getDataFromSource(sourcesFinal("aa001163_glb_cds_well_class")))

        val dfWellRefPoint = transformationDfRefPoint(getDataFromSource(sourcesFinal("aa001163_glb_cds_well_reference_point")))

        df= dfWell.join(dfInstWellConn, "cod_uwi", "left")
        //df.printSchema

        df = df.join(dfWellAlias, "cod_uwi", "left")

        df = df.join(dfWellSurfLoc, "cod_uwi", "left")

        df = df.join(dfWellClass, "cod_uwi", "left")

        df = df.join(dfWellRefPoint, "cod_uwi", "left")
        //.drop("cod_api10")
        //.drop("des_installation_name")

        df= assembleWellClass(df)

        //df= renameDf(dfWell)


df = nullifyOutOfRangeDates(df)
//df.printSchema
        df = df.select(orderFields.map(col): _*)
        
        //df.printSchema
//display(df)
        //df.printSchema
println("a")
val dupCount = countDuplicates(df)
println(s"Número de filas duplicadas: $dupCount")
        val count= df.count
        println(count)
        //df.groupBy("ind_active_loc").count().show(false)

        // writeDataFrame(df, targetFinal)
        val dfFormatted = applyDDLFromTableToDF(df,getObject((target_object).toString))
        writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)
        //checkWellHeaderIssues(df)
        //checkTypeCastingIssues(df, bbddInfo)
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


val result = s"""{"error": "$error"}"""

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

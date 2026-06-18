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

import org.apache.spark.sql.functions._
def renameColumns(df: DataFrame): DataFrame = {

  val columnasAConsiderar = Seq("BaseModel","Blue","Brallier","Brown","Cherry Valley","Geneseo","Green","Lock Haven","Moscow","Moscow DM A","Moscow DM B","Moscow DM C","No Go","Onondaga","Orange",
                              "Red","Tully","Union Springs","Upper Marcellus","AC_0","AC_10","AC_20","Anacacho","AustinChalk","Buda","CookMountain","DelRio","EagleFordLower","EagleFordMiddle",
                              "EagleFordUpper","Edwards","Escondido","Georgetown","LEF_mfs","LEF_sb0","LEF_sb1","LEF_sb2","LEF_sb3","Lower_TZ_Base","Lower_TZ_Top","Maness","Marker","Midway",
                              "Navarro","No Go EFD","Olmos","Pearsall","Queen City","Reklaw","SanMiguel","Sligo","Sparta","Taylor","Upson","Vicksburg","Weches","WilcoxUpper","WilcoxLower","WilcoxMiddle","Yegua")
  columnasAConsiderar.foldLeft(df) { (dfTemp, colName) =>
  // Convertimos el nombre de la columna a minúsculas y reemplazamos espacios por guiones bajos
    val newColName = s"des_hor_${colName.toLowerCase.replace(" ", "_")}"
    dfTemp.withColumnRenamed(colName, newColName)
  }
}


val orderFields: Seq[String] = Seq("cod_vers_wellbore", "cod_vers_well_string", "cod_vers_well", "cod_uwbi" ,"cod_api12", "val_md", "val_z", "val_azimuth", "val_inclination", "val_vs_azim_azimuth", "val_vs_azim_vs", "des_hor_ac_0", 
                                   "des_hor_ac_10", "des_hor_ac_20", "des_hor_anacacho", "des_hor_austinchalk", "des_hor_basemodel", "des_hor_blue", "des_hor_brallier", "des_hor_brown", "des_hor_buda", 
                                   "des_hor_cherry_valley", "des_hor_cookmountain", "des_hor_delrio", "des_hor_eaglefordlower", "des_hor_eaglefordmiddle", "des_hor_eaglefordupper", "des_hor_edwards",
                                   "des_hor_escondido", "des_hor_geneseo", "des_hor_georgetown", "des_hor_green", "des_hor_lef_mfs", "des_hor_lef_sb0", "des_hor_lef_sb1", "des_hor_lef_sb2", "des_hor_lef_sb3", 
                                   "des_hor_lock_haven", "des_hor_lower_tz_base", "des_hor_lower_tz_top", "des_hor_maness", "des_hor_midway", "des_hor_moscow", "des_hor_moscow_dm_a", "des_hor_moscow_dm_b",
                                   "des_hor_moscow_dm_c", "des_hor_navarro", "des_hor_no_go", "des_hor_no_go_efd", "des_hor_olmos", "des_hor_onondaga", "des_hor_orange", "des_hor_queen_city", "des_hor_red",
                                   "des_hor_reklaw", "des_hor_sanmiguel", "des_hor_sparta", "des_hor_taylor", "des_hor_tully", "des_hor_union_springs", "des_hor_upper_marcellus", "des_hor_upson", "des_hor_weches", 
                                   "des_hor_wilcoxlower", "des_hor_wilcoxmiddle", "des_hor_wilcoxupper", "des_hor_yegua", "des_hor_marker", "des_hor_pearsall", "des_hor_sligo", "des_hor_vicksburg", "des_geosteering_log", 
                                   "bol_flag_cambio", "cod_api", "val_geosteering_x_coord", "val_geosteering_y_coord", "val_geosteering_latitude", "val_geosteering_longitude")

def renameFinalColumns (df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn.withColumnRenamed("md_val", "val_md")
               .withColumnRenamed("z_val", "val_z")
               .withColumnRenamed("azimuth_val", "val_azimuth")
               .withColumnRenamed("inclination_val", "val_inclination")
               .withColumnRenamed("vs_azim_azimuth_val", "val_vs_azim_azimuth")
               .withColumnRenamed("vs_azim_vs_val", "val_vs_azim_vs")
  return dfTrn 
}
 

def transformationStarred(df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn.withColumnRenamed("interpretation", "interpretations_uuid").select("interpretations_uuid", "wells_uuid").dropDuplicates

  return dfTrn
}

def transformationWells (df: DataFrame): DataFrame = {

  var dfTrn: DataFrame = df

  dfTrn = dfTrn.select("wells_uuid", "api")
               .withColumnRenamed("api","cod_api")
               .withColumn("cod_api_fix", regexp_replace(col("cod_api"), "[^0-9]", ""))
               .withColumn("cod_api12", when(length(col("cod_api_fix")) === 10, concat(col("cod_api_fix"), lit("00")))
               .otherwise(substring(col("cod_api_fix"),1,12 )))
               .select("wells_uuid", "cod_api", "cod_api12")
               .dropDuplicates("wells_uuid", "cod_api12")

  return dfTrn

      
}


def transformationDimWellbore (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.select("fec_start_date_rel", "id_vers_rel", "cod_uwbi", "cod_api12", "id_vers_well", "id_vers_well_hole", "des_business_unit")

    return dfTrn
}

def join (dfL: DataFrame, dfR: DataFrame, key: String): DataFrame = {

    var dfTrn: DataFrame = null
    dfTrn = dfL.join(dfR, Seq(key), "inner")

return dfTrn
}


def joinWellbore (dfL: DataFrame, dfR: DataFrame): DataFrame = {

    val  windowFilter = Window.partitionBy("interpretations_uuid", "md_val").orderBy(desc("fec_start_date_rel"))

    var dfTrn: DataFrame = null

    dfTrn = dfL.join(dfR, Seq("cod_api12"), "left").withColumn("cod_vers_wellbore", coalesce(dfR("id_vers_rel"), lit("-99"))).withColumn("cod_vers_well_string", coalesce(dfR("id_vers_well"), lit("-99"))).withColumn("cod_vers_well", coalesce(dfR("id_vers_well_hole"), lit("-99")))
    
    val windowDF = dfTrn.withColumn("row_number",row_number().over(windowFilter)).filter("row_number == 1")

    return windowDF
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Función de transformación de Horizons

// COMMAND ----------

def transformationHorizons(df: DataFrame): DataFrame = {

  // Definir horizonsMap y convertirlo a un DataFrame
  val horizonsMap: Map[List[String], String] = Map(
    List("Lock Haven")      -> "Lock Haven",
    List("Brallier")        -> "Brallier",
    List("Geneseo")         -> "Geneseo",
    List("Tully")           -> "Tully",
    List("Moscow")          -> "Moscow",
    List("Moscow DM C")     -> "Moscow DM C",
    List("Moscow DM B")     -> "Moscow DM B",
    List("Moscow DM A")     -> "Moscow DM A",
    List("Upper Marcellus") -> "Upper Marcellus",
    List("Cherry Valley")   -> "Cherry Valley",
    List("Union Springs")   -> "Union Springs",
    List("Red")             -> "Red",
    List("Blue")            -> "Blue",
    List("Green")           -> "Green",
    List("Brown")           -> "Brown",
    List("Orange")          -> "Orange",
    List("No Go")           -> "No Go",
    List("Onondaga")        -> "Onondaga",
    List("BaseModel")       -> "BaseModel",
    List("AC 0", "AC 0_AS", "AC_0", "AC_0_AS") -> "AC_0",
    List("AC 10", "AC 10_AS", "AC_10", "AC_10_AS") -> "AC_10",
    List("AC 20", "AC 20_AS", "AC_20", "AC_20_AS") -> "AC_20",
    List("Anacacho", "Anacacho_AS", "ZoneMarker(ANACACHO)") -> "Anacacho",
    List("AUSTIN CHALK", "AustinChalk", "AustinChalk_AS", "ZoneMarker(AUSTIN_CHALK)") -> "AustinChalk",
    List("Buda","Buda/EagleFordBase","Buda/EagleFordBase_AS","Buda_AS","Eagle Ford Base","EagleFordBase","EagleFordBase/Buda","EagleFordBase_AS") -> "Buda",
    List("COOK_MOUNTAIN_ACOUSTIC", "COOK_MOUNTAIN_ACOUSTIC_AS", "CookMountain") -> "CookMountain",
    List("DelRio", "DelRio_AS") -> "DelRio",
    List("Eagle Ford Lower", "EagleFordLower","EagleFordLower_AS","LOWER EAGLE FORD","Lower EFB","Lower EFB_AS","ZoneMarker(EAGLE_FORD_LOWER)") -> "EagleFordLower",
    List("Eagle Ford Middle","Eagle Ford Middle/Lower","Eagle Frod Middle","EagleFordMiddle","EagleFordMiddle_AS","ZoneMarker(EAGLE_FORD_MIDDLE)" ) -> "EagleFordMiddle",
    List("Eagle Ford Upper","EagleFordUpper","EagleFordUpper_AS","UPPER EAGLE FORD","ZoneMarker(EAGLE_FORD_UPPER)") -> "EagleFordUpper",
    List("Edwards", "Edwards_AS") -> "Edwards",
    List("Escondido", "Escondido_AS") -> "Escondido",
    List("Georgetown", "Georgetown_AS") -> "Georgetown",
    List("LEF mfs", "LEF mfs_AS", "LEF_mfs", "LEF_mfs_AS") -> "LEF_mfs",
    List("LEF SB0", "LEF_sb0", "LEF_sb0_AS") -> "LEF_sb0",
    List("LEF sb1","LEF SB1_AS","LEF_sb1","LEF_sb1_AS","SB1","SB1_AS","SB1_TARGET_LEF_BASE") -> "LEF_sb1",
    List("LEF SB2", "LEF SB2_AS", "LEF_sb2", "LEF_sb2_AS") -> "LEF_sb2",
    List("LEF sb3", "LEF SB3_AS", "LEF_sb3", "LEF_sb3_AS") -> "LEF_sb3",
    List("Lower Target Base", "Lower TZ Base", "Lower TZ Base_AS") -> "Lower_TZ_Base",
    List("Lower Target Top", "Lower TZ Top", "Lower TZ Top_AS") -> "Lower_TZ_Top",
    List("Maness", "Maness_AS") -> "Maness",
    List("Marker") -> "Marker",
    List("Midway", "Midway_AS", "ML Midway", "ML Midway_AS") -> "Midway",
    List("Navarro", "Navarro_AS") -> "Navarro",
    List("NO GO","NO GO_AS","NoGo","No-Go","NO-GO!","NoGo_AS","No-Go_AS","ZoneMarker(EAGLE_FORD_BASE)") -> "No Go EFD",
    List("Olmos", "Olmos_AS", "ZoneMarker(OLMOS)") -> "Olmos",
    List("Pearsall") -> "Pearsall",
    List("Queen City", "Queen City_AS", "QueenCity", "QueenCity_AS") -> "Queen City",
    List("Reklaw", "Reklaw_AS") -> "Reklaw",
    List("SanMiguel", "SanMiguel_AS") -> "SanMiguel",
    List("Sligo") -> "Sligo",
    List("Sparta", "Sparta_AS") -> "Sparta",
    List("TAYLOR", "TAYLOR_AS") -> "Taylor",
    List("Upson", "Upson_AS", "ZoneMarker(UPSON)") -> "Upson",
    List("Vicksburg") -> "Vicksburg",
    List("Weches", "Weches_AS") -> "Weches",
    List("WILCOX","Wilcox Marker","Wilcox Marker_AS","Wilcox Upper","Wilcox Upper_AS","WILCOX_UPPER_ACOUSTIC","WilcoxUpper","WilcoxUpper_AS") -> "WilcoxUpper",
    List("WilcoxLower", "WilcoxLower_AS", "WilcoxLowerMarker", "WilcoxLowerMarker_AS") -> "WilcoxLower",
    List("WilcoxMiddle", "WilcoxMiddle_AS") -> "WilcoxMiddle",
    List("Yegua", "Yegua_AS") -> "Yegua"
  )

  // Convertir horizonsMap a un DataFrame para realizar el join
  val horizonsList = horizonsMap.flatMap { case (keys, value) => keys.map(key => (key, value)) }.toSeq
  val horizonsDF = horizonsList.toDF("horizon_name", "mapped_value")

  // Definir el esquema del array
  val tvdssSchema = StructType(Seq(
    StructField("val", DoubleType, nullable = true)
  ))

  val elementSchema = StructType(Seq(
    StructField("name", StringType, nullable = true),
    StructField("tvdss", tvdssSchema, nullable = true),
    StructField("uuid", StringType, nullable = true)
  ))

  val arraySchema = ArrayType(elementSchema, containsNull = true)

  // Definir las columnas que se van a considerar
  val columnasAConsiderar = Seq(
    "BaseModel", "Blue", "Brallier", "Brown", "Cherry Valley", "Geneseo", "Green", "Lock Haven", "Moscow",
    "Moscow DM A", "Moscow DM B", "Moscow DM C", "No Go", "Onondaga", "Orange", "Red", "Tully",
    "Union Springs", "Upper Marcellus", "AC_0", "AC_10", "AC_20", "Anacacho", "AustinChalk", "Buda",
    "CookMountain", "DelRio", "EagleFordLower", "EagleFordMiddle", "EagleFordUpper", "Edwards", "Escondido",
    "Georgetown", "LEF_mfs", "LEF_sb0", "LEF_sb1", "LEF_sb2", "LEF_sb3", "Lower_TZ_Base", "Lower_TZ_Top",
    "Maness", "Marker", "Midway", "Navarro", "No Go EFD", "Olmos", "Pearsall", "Queen City", "Reklaw", "SanMiguel",
    "Sligo", "Sparta", "Taylor", "Upson", "Vicksburg", "Weches", "WilcoxUpper", "WilcoxLower", "WilcoxMiddle", "Yegua"
  )

  // Crear array de estructuras dinámicamente
  val arrayDeEstructuras = array(
    columnasAConsiderar.map { columna =>
      struct(col(columna).as("valor"), lit(columna).as("nombre"))
    }: _* // Desempaqueta la secuencia para pasarlo como argumentos individuales
  ).as("mapped_values")

  // Inicializar DataFrame transformado
  var dfTrn: DataFrame = df

  // Reparticionar el DataFrame para optimizar el rendimiento
  dfTrn = dfTrn.repartition(100, col("interpretations_uuid"))

  // Definir la ventana para las funciones de lag
  val horizonWindow = Window.partitionBy("interpretations_uuid").orderBy("md_val")

  // Transformaciones
  dfTrn = dfTrn
    .withColumn("parsed_array", from_json(col("horizons"), arraySchema))
    .withColumn("element", explode(col("parsed_array")))
    .withColumn("horizon_name", col("element.name"))
    .withColumn("tvdss", col("element.tvdss.val"))
    .join(horizonsDF, Seq("horizon_name"), "inner")  // Join para mapear los valores
    .groupBy("interpretations_uuid", "md_val", "z_val", "azimuth_val", "inclination_val", "vs_azim_azimuth_val", "vs_azim_vs_val")
    .pivot("mapped_value")
    .agg(first("tvdss"))

  // Añadir columnas que falten con valores nulos
  columnasAConsiderar.foreach { field =>
    if (!dfTrn.columns.contains(field)) {
      dfTrn = dfTrn.withColumn(field, lit(null).cast("double"))
    }
  }

  // Aplicar las transformaciones finales
  dfTrn = dfTrn
    .withColumn("mapped_values", arrayDeEstructuras)  // Añadir array de estructuras

    // 1. Filtrar valores **mayores que z_val** en lugar de menores
    .withColumn("filtered_mapped", expr("filter(mapped_values, x -> x.valor > z_val)"))  // **Cambio realizado aquí**

    // 2. Ordenar de manera **ascendente** para obtener el menor valor mayor que z_val
    .withColumn("sorted_mapped", sort_array(col("filtered_mapped")))  // **Cambio realizado aquí**

    // 3. Extraer el nombre del primer elemento (el menor valor mayor que z_val)
    .withColumn("des_geosteering_log", expr("IF(size(sorted_mapped) > 0, sorted_mapped[0].nombre, null)"))  // Extraer el nombre correspondiente

    // Eliminar columnas intermedias que ya no son necesarias
    .drop("mapped_values", "filtered_mapped", "sorted_mapped")

    // Aplicar función de lag para obtener el valor anterior de des_geosteering_log
    .withColumn("lag_geosteering_log", coalesce(lag(col("des_geosteering_log"), 1).over(horizonWindow), col("des_geosteering_log")))

    // Crear una bandera que indique si hubo un cambio en des_geosteering_log respecto al valor anterior
    .withColumn("bol_flag_cambio", when(
      (col("lag_geosteering_log").isNull && col("des_geosteering_log").isNull) ||
      (col("lag_geosteering_log") === col("des_geosteering_log")),
      lit(0)
    ).otherwise(lit(1)))

  // Retornar el DataFrame transformado
  return dfTrn
}


// COMMAND ----------

// spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")

import java.lang.Math

val outputSchema = StructType(Seq(
  StructField("interpolatedIncDeg", DoubleType, nullable = true),
  StructField("interpolatedAziDeg", DoubleType, nullable = true),
  StructField("md1", DoubleType, nullable = true),
  StructField("inc1Deg", DoubleType, nullable = true),
  StructField("azi1Deg", DoubleType, nullable = true),
  StructField("tvd1", DoubleType, nullable = true),
  StructField("accumulatedX", DoubleType, nullable = true),
  StructField("accumulatedY", DoubleType, nullable = true),
  StructField("interpolatedTVD", DoubleType, nullable = true),
  StructField("tvdAbsolute", DoubleType, nullable = true) // Nuevo campo
))

// Función para interpolar ángulos considerando su naturaleza circular
def interpolateAngle(angle1Deg: java.lang.Double, angle2Deg: java.lang.Double, ratio: java.lang.Double): java.lang.Double = {
  val deltaAngle = ((angle2Deg - angle1Deg + 540) % 360) - 180 // diferencia angular mínima
  val interpolatedAngle = (angle1Deg + ratio * deltaAngle + 360) % 360
  interpolatedAngle
}

val minCurvatureInterpolateWithXY = udf(
  (
    md: java.lang.Double,
    mdArray: Seq[java.lang.Double],
    incArray: Seq[java.lang.Double],
    aziArray: Seq[java.lang.Double],
    tvdArray: Seq[java.lang.Double],
    initialX: java.lang.Double,
    initialY: java.lang.Double,
    xArray: Seq[java.lang.Double],
    yArray: Seq[java.lang.Double],
    datumElevation: java.lang.Double // Nuevo parámetro
  ) => {
    // Validación de nulos y tamaños de arrays
    if (md == null || mdArray == null || incArray == null || aziArray == null || tvdArray == null || datumElevation == null) {
      Row(null, null, null, null, null, null, null, null, null, null)
    } else {
      val n = mdArray.size
      if (n == 0 || incArray.size != n || aziArray.size != n || tvdArray.size != n) {
        Row(null, null, null, null, null, null, null, null, null, null)
      } else {
        val calculateXY = initialX != null && initialY != null && xArray != null && yArray != null && xArray.size == n && yArray.size == n

        if (md <= mdArray.head) {
          val firstX = if (calculateXY) initialX else null
          val firstY = if (calculateXY) initialY else null
          val interpolatedTVD = tvdArray.head
          val tvdAbsolute = datumElevation - interpolatedTVD
          Row(incArray.head, aziArray.head, mdArray.head, incArray.head, aziArray.head, tvdArray.head,
              firstX, firstY, interpolatedTVD, tvdAbsolute)
        } else if (md >= mdArray.last) {
          val lastX = if (calculateXY) xArray.last else null
          val lastY = if (calculateXY) yArray.last else null
          val interpolatedTVD = tvdArray.last
          val tvdAbsolute = datumElevation - interpolatedTVD
          Row(aziArray.last, aziArray.last, mdArray.last, incArray.last, aziArray.last, tvdArray.last,
              lastX, lastY, interpolatedTVD, tvdAbsolute)
        } else {
          val idxOption = mdArray.indexWhere(_ > md) match {
            case -1 => None
            case idx  => Some(idx)
          }
          idxOption match {
            case Some(idx) if idx > 0 =>
              val md1 = mdArray(idx - 1)
              val md2 = mdArray(idx)
              val inc1Deg = incArray(idx - 1)
              val inc2Deg = incArray(idx)
              val azi1Deg = aziArray(idx - 1)
              val azi2Deg = aziArray(idx)
              val tvd1 = tvdArray(idx - 1)
              val x1 = if (calculateXY) xArray(idx - 1) else null
              val y1 = if (calculateXY) yArray(idx - 1) else null
              val ratio = (md - md1) / (md2 - md1)
              val interpolatedIncDeg = inc1Deg + ratio * (inc2Deg - inc1Deg)
              val interpolatedAziDeg = interpolateAngle(azi1Deg, azi2Deg, ratio)
              val inc1Rad = Math.toRadians(inc1Deg)
              val inc2Rad = Math.toRadians(interpolatedIncDeg)
              val azi1Rad = Math.toRadians(azi1Deg)
              val azi2Rad = Math.toRadians(interpolatedAziDeg)
              val cosDogLegSeverity = Math.cos(inc2Rad - inc1Rad) - Math.sin(inc1Rad) * Math.sin(inc2Rad) * (1 - Math.cos(azi2Rad - azi1Rad))
              val cosDogLegSeverityClipped = Math.max(-1.0, Math.min(1.0, cosDogLegSeverity))
              val dogLegSeverity = Math.acos(cosDogLegSeverityClipped)
              val dogLegFactor = if (dogLegSeverity != 0 && !dogLegSeverity.isNaN) (2 / dogLegSeverity) * Math.tan(dogLegSeverity / 2) else 1.0
              val deltaMD = md - md1
              val deltaX: java.lang.Double = if (calculateXY) {
                (deltaMD / 2 * (Math.sin(inc1Rad) * Math.sin(azi1Rad) + Math.sin(inc2Rad) * Math.sin(azi2Rad)) * dogLegFactor).toDouble
              } else null
              val deltaY: java.lang.Double = if (calculateXY) {
                (deltaMD / 2 * (Math.sin(inc1Rad) * Math.cos(azi1Rad) + Math.sin(inc2Rad) * Math.cos(azi2Rad)) * dogLegFactor).toDouble
              } else null
              val accumulatedX: java.lang.Double = if (calculateXY && x1 != null && deltaX != null) x1 + deltaX else null
              val accumulatedY: java.lang.Double = if (calculateXY && y1 != null && deltaY != null) y1 + deltaY else null
              val deltaTVD = deltaMD / 2 * (Math.cos(inc1Rad) + Math.cos(inc2Rad)) * dogLegFactor
              val interpolatedTVD = tvd1 + deltaTVD
              val tvdAbsolute = interpolatedTVD - datumElevation
              Row(interpolatedIncDeg, interpolatedAziDeg, md1, inc1Deg, azi1Deg, tvd1,
                  accumulatedX, accumulatedY, interpolatedTVD, tvdAbsolute)
            case _ =>
              Row(null, null, null, null, null, null, null, null, null, null)
          }
        }
      }
    }
  },
  outputSchema
)


// COMMAND ----------

def transformationDevSurvey (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.orderBy("cod_uwbi", "val_station_md").groupBy("cod_uwbi")
                .agg(
                    collect_list("val_station_md").as("mdArray"),
                    collect_list("val_station_dev").as("incArray"),
                    collect_list("val_station_az_grid").as("aziArray"),
                    collect_list("val_station_tvd").as("tvdArray"),
                    collect_list("x_interpolado").as("xArray"),
                    collect_list("y_interpolado").as("yArray"),
                    first("val_preferred_easting").as("val_preferred_easting"),
                    first("val_preferred_northing").as("val_preferred_northing"),
                    first("val_datum_elevation").as("val_datum_elevation"))
                 

    return dfTrn

}

def transformationFinalStimTreatTrn (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("interpolatedIncDeg", "val_geosteering_dev")
                 .withColumnRenamed("interpolatedAziDeg", "val_geosteering_az")
                 .withColumnRenamed("accumulatedX", "val_geosteering_x_coord")
                 .withColumnRenamed("accumulatedY", "val_geosteering_y_coord")
                 .withColumnRenamed("interpolatedTVD", "val_geosteering_depth_tvd")
                 .withColumnRenamed("tvdAbsolute", "val_geosteering_depth_abs_tvd")
                 .drop("interpolatedData", "val_datum_elevation", "md1", "inc1Deg", "azi1Deg", "mdArray", "incArray", "aziArray", "tvdArray", "xArray", "yArray", "val_preferred_easting", "val_preferred_northing", "tvd1")

    return dfTrn
}


// COMMAND ----------

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.locationtech.proj4j.{CRSFactory, CoordinateReferenceSystem, ProjCoordinate, CoordinateTransformFactory}

// Creamos un objeto que inicializa (una sola vez por ejecutor) las fábricas y transformaciones.
object CoordinateTransformer {
  lazy val crsFactory = new CRSFactory()
  lazy val transformFactory = new CoordinateTransformFactory()
  lazy val efTransform = transformFactory.createTransform(
    crsFactory.createFromName("EPSG:32040"), // NAD27 / UTM zone 14N
    crsFactory.createFromName("EPSG:4326")    // WGS84
  )
  lazy val mbuTransform = transformFactory.createTransform(
    crsFactory.createFromName("EPSG:3363"), // NAD83 / UTM zone 18N
    crsFactory.createFromName("EPSG:4326")    // WGS84
  )
}

def utmToLatLonManual(utmX: Double, utmY: Double, systemType: String): (Double, Double) = {
  val transform = systemType match {
    case "EF"  => CoordinateTransformer.efTransform
    case "MBU" => CoordinateTransformer.mbuTransform
    case _     => null
  }
  if (transform == null) {
    (Double.NaN, Double.NaN)
  } else {
    val srcCoord = new ProjCoordinate(utmX, utmY)
    val destCoord = new ProjCoordinate()
    transform.transform(srcCoord, destCoord)
    (destCoord.y, destCoord.x)
  }
}

val utmToLatLonUDF = udf((utmX: Double, utmY: Double, state: String) => {
  val systemType = state match {
    case "EagleFord"  => "EF"
    case "Marcellus"  => "MBU"
    case _            => ""
  }
  val (lat, lon) = utmToLatLonManual(utmX, utmY, systemType)
  (lat, lon)
})

def addLatLonColumns(df: DataFrame): DataFrame = {
  df.withColumn("lat_lon", utmToLatLonUDF(col("val_geosteering_x_coord"), col("val_geosteering_y_coord"), col("des_business_unit")))
    .withColumn("val_geosteering_latitude", col("lat_lon._1"))
    .withColumn("val_geosteering_longitude", col("lat_lon._2"))
    .drop("lat_lon")
}


// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.DataFrame

var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {
  // Obtención y transformación de los DataFrames de origen
  val dfHor       = transformationHorizons(getDataFromSource(sourcesFinal("`aa000935|glb|horizons`")))
  val dfStar      = transformationStarred(getDataFromSource(sourcesFinal("`aa000935|glb|starred`")))
  val dfWell      = transformationWells(getDataFromSource(sourcesFinal("`aa000935|glb|well`")))
  val dfDimWellbore = transformationDimWellbore(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_wellbore_un")))
  val dfDevSurvey = transformationDevSurvey(getDataFromSource(sourcesFinal("am_eyp0007_trn_tb_fac_d_def_surv_stat")))
  val dfStgFin = getDataFromSource(sourcesFinal("eyp0007_tb_fac_d_comp_stg_final"))

  // Se realizan los joins y transformaciones. Aquí se agrupa la cadena de operaciones
  df = dfHor
         .join(dfStar, "interpretations_uuid")
         .join(dfWell, "wells_uuid")
         .transform(df => joinWellbore(df, dfDimWellbore))
         // .transform(castDoubleToDouble)
         .transform(renameColumns)
         .join(
            dfDevSurvey.withColumn("val_datum_elevation", col("val_datum_elevation").cast("double")),
            Seq("cod_uwbi"), "left"
         )
         .withColumn("md_val", col("md_val").cast("double"))

  // Cacheamos en este punto para no repetir las operaciones anteriores en las siguientes acciones
  df = df.cache()

  // Aplicamos la interpolación y extraemos los campos del Row resultante
  df = df.withColumn("interpolatedData", minCurvatureInterpolateWithXY(
            col("md_val"), col("mdArray"), col("incArray"), 
            col("aziArray"), col("tvdArray"), col("val_preferred_easting"),
            col("val_preferred_northing"), col("xArray"), col("yArray"), col("val_datum_elevation")
         ))
         .selectExpr("*", "interpolatedData.*")
  
  df = renameFinalColumns(df)

  df = transformationFinalStimTreatTrn(df)

  df = addLatLonColumns(df)

  df = df

  df = df.select(orderFields.map(col): _*)
  df.printSchema

  val dfFormatted = applyDDLFromTableToDF(df,getObject((target_object).toString))
  writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

  // Si es necesario conocer la cantidad de filas escritas, se recomienda obtener el count antes de liberar el cache
  val countWrites = df.count()
  df.unpersist()

  println(s"Process complete with: $countWrites written rows")
} catch {
  case e: Throwable =>
    error = e.toString
    println(error)
    LogHelper().logEndKO(error)
}



// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------


val result = s"""{"error": "$error"}"""

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

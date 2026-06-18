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

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._ 

val orderFields: Seq[String] = Seq("cod_vers_wellbore", "cod_uwbi", "val_md", "val_Temperature", "val_GR", "val_GR_at_bit", "val_ROP", "val_RPM", "val_SPP", "val_WOB", 
"val_Flow_Out", "val_Total_Gas", "val_C1", "val_C2", "val_C3", "val_C4", "val_iC4", "val_nC4", "val_C5", "val_Hole_Diameter", "val_Date", "val_Time")



def transformlog(df: DataFrame): DataFrame = {

    val dflogg = df.select("wells_uuid", "name", "logs_uuid")
    dflogg.dropDuplicates
}

def transformlogdata(df: DataFrame): DataFrame = {

  val dflogdatas = df.withColumn("date", to_timestamp(concat_ws("-", col("year_load_aria"), col("month_load_aria"), col("day_load_aria")), "yyyy-M-d"))
  dflogdatas
}

def joinlogs(df1: DataFrame, df2: DataFrame): DataFrame = {     


    val dflogsjoin = df1.join(df2, Seq("logs_uuid"), "left").drop("year_load_aria","month_load_aria","day_load_aria")
    dflogsjoin
}

def joinwell(df1: DataFrame, df2: DataFrame): DataFrame = {

    val dfselected = df2.select("wells_uuid","api")
    val df2drop = dfselected.dropDuplicates()

    val dfjoin = df1.join(df2drop, Seq("wells_uuid"), "left")
    dfjoin
}

def transformWellb(df: DataFrame): DataFrame = {

    val windowSpec = Window.partitionBy("cod_uwbi", "cod_api12").orderBy(col("fec_start_date_rel").desc)

    val dfwellbselect = df.select("id_vers_rel", "cod_uwbi", "cod_api12", "fec_start_date_rel")
    val dfRanked = dfwellbselect.withColumn("row_num", row_number().over(windowSpec))
    val dfresult = dfRanked.filter(col("row_num") === 1).drop("row_num")

    dfresult

}

def JoinlogsWellb(df1: DataFrame, df2: DataFrame): DataFrame = {

    val df11 = df1.withColumn("api", col("api").substr(1, 12))
    val dfJoin = df11.join(df2, df11("api") === df2("cod_api12"), "left")
    .withColumn("cod_vers_wellbore", coalesce(df2("id_vers_rel"), lit("-99"))).withColumn("cod_uwbi", coalesce(df2("cod_uwbi"), lit("-99"))).drop("fec_start_date_rel","id_vers_rel")
    dfJoin
    }
def uppercaseNameColumn(df: DataFrame): DataFrame = {
  df.withColumn("name", upper(col("name")))
}
def pivotDataFrame(df: DataFrame): DataFrame = {
  df.groupBy("cod_vers_wellbore","cod_uwbi", "md_val") // Agrupar por llaves primarias
    .pivot("name") // Convertir valores únicos de 'name' en columnas
    .agg(first("data_val")) // Llenar con valores de 'data_val'
}

def applyColumnMappings(df: DataFrame): DataFrame = {
  // Mapeo de columna destino a una secuencia de columnas fuente ordenadas por prioridad
  val columnMappingsMulti: Map[String, Seq[String]] = Map(
    "Depth_temporal" -> Seq("DEPT"),
    "Temperature_temporal" -> Seq("TEMP", "T-TEMP"),
    "GR_temporal" -> Seq("GR", "GAM", "GAMMA", "GRCX", "GRAX", "GNCGR", "GR_RM", "GR_MWD", "GR_RT", "GR_spliced", "GR_BHA3", "GR_BHA_5", "GR_BHA7", "GR_BHA2", "GR_BHA1", "GR_BHA6"),
    "GR_at_bit_temporal" -> Seq("GAMB"),
    "ROP_temporal" -> Seq("ROP"),
    "RPM_temporal" -> Seq("RPM"),
    "SPP_temporal" -> Seq("SPP"),
    "WOB_temporal" -> Seq("WOB"),
    "flow_out_temporal" -> Seq("TPO"),
    "total_gas_temporal" -> Seq("PGAS", "3GAS"),
    "C1_temporal" -> Seq("PC1G"),
    "C2_temporal" -> Seq("PC2G"),
    "C3_temporal" -> Seq("PC3G"),
    "C4_temporal" -> Seq("PC4G"),
    "iC4_temporal" -> Seq("PIC4G"),
    "nC4_temporal" -> Seq("PNC4G"),
    "C5_temporal" -> Seq("PC5G"),
    "hole_diameter_temporal" -> Seq("HDIAM"),
    "date_temporal" -> Seq("YYMMDD"),
    "time_temporal" -> Seq("HHMMSS")
  )

  // Obtener la lista de columnas del DataFrame una sola vez
  val dfColumns: Array[String] = df.columns

  // Para cada mapeo, generar la expresión que calcula la columna nueva
  val newColumnsExpr: Seq[Column] = columnMappingsMulti.map { case (newCol, candidateCols) =>
    val existingCandidates = candidateCols.filter(dfColumns.contains)
    if (existingCandidates.nonEmpty) {
      // Se construye coalesce para las columnas existentes, casteándolas a Double
      coalesce(existingCandidates.map(c => col(c).cast("Double")): _*).alias(newCol)
    } else {
      // Si ninguna columna está presente, se asigna null
      lit(null).cast("Double").alias(newCol)
    }
  }.toSeq

  // Se combinan las columnas originales con las nuevas en una sola operación de select.
  // Si deseas conservar todas las columnas originales, haz:
  df.select(dfColumns.map(col) ++ newColumnsExpr: _*)
  
  // Si, en cambio, solo te interesa agregar las columnas nuevas, podrías
  // hacer un join o concatenar columnas; sin embargo, en la mayoría de casos se prefiere conservar las originales.
}


def seleccionarColumnas(df: DataFrame): DataFrame = {
  val columnasTemporal = df.columns.filter(_.endsWith("_temporal"))
  val columnasSeleccionadas = Seq("cod_vers_wellbore","cod_uwbi", "md_val") ++ columnasTemporal
  df.select(columnasSeleccionadas.map(col): _*)
}

def transformarColumnas(df: DataFrame): DataFrame = {
  // Mapeo de nombres originales a los nuevos nombres
  val renombramientos = Map(
    "cod_vers_wellbore" -> "cod_vers_wellbore",
    "cod_uwbi" -> "cod_uwbi",
    "md_val" -> "val_md",
    "Temperature_temporal" -> "val_Temperature",
    "GR_temporal" -> "val_GR",
    "GR_at_bit_temporal" -> "val_GR_at_bit",
    "ROP_temporal" -> "val_ROP",
    "RPM_temporal" -> "val_RPM",
    "SPP_temporal" -> "val_SPP",
    "WOB_temporal" -> "val_WOB",
    "flow_out_temporal" -> "val_Flow_Out",
    "total_gas_temporal" -> "val_Total_Gas",
    "C1_temporal" -> "val_C1",
    "C2_temporal" -> "val_C2",
    "C3_temporal" -> "val_C3",
    "C4_temporal" -> "val_C4",
    "iC4_temporal" -> "val_iC4",
    "nC4_temporal" -> "val_nC4",
    "C5_temporal" -> "val_C5",
    "hole_diameter_temporal" -> "val_Hole_Diameter",
    "date_temporal" -> "val_Date",
    "time_temporal" -> "val_Time"
  )

  // Aplicar los cambios de nombres en el DataFrame
  val dfRenombrado = renombramientos.foldLeft(df) {
    case (tempDf, (colOriginal, colNuevo)) =>
      if (tempDf.columns.contains(colOriginal)) tempDf.withColumnRenamed(colOriginal, colNuevo)
      else tempDf
  }

  dfRenombrado
}


// COMMAND ----------

/** Reemplaza puntos, comas y espacios por "_" y colapsa guiones bajos repetidos */
def sanitizeDots(df: DataFrame): DataFrame = {
  df.withColumn("name", {
    // 1) sustituye . , y espacios por _
    val step1 = regexp_replace(col("name"), "[\\. ,]+", "_")
    // 2) colapsa múltiples _ seguidos en uno solo
    val step2 = regexp_replace(step1, "_+", "_")
    // 3) quita _ al principio o final
    val trimmed = regexp_replace(regexp_replace(step2, "^_", ""), "_$", "")
    trimmed.alias("name")
  })
}


// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {   

        val log = transformlog(getDataFromSource(sourcesFinal("`aa000935|glb|log`")))

        val logdata = transformlogdata(getDataFromSource(sourcesFinal("aa000935_glb_log_data")))

        val dflogsdata = joinlogs(logdata, log)

        val dfwellb = transformWellb(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_wellbore_un")))

        df = joinwell(dflogsdata, getDataFromSource(sourcesFinal("`aa000935|glb|well`")))

        df = JoinlogsWellb(df,dfwellb)
        df = df.withColumn(
  "name_clean", 
  regexp_replace(regexp_replace(col("name"), "[\\.\\s]", "_"), "_+", "_")
)

        df = df.filter(!col("name").rlike("[\\:\\(\\)\\/\\[\\]]"))

        df = sanitizeDots(df)

        df = uppercaseNameColumn(df)

        df = pivotDataFrame(df)

        df = applyColumnMappings(df)

        df = seleccionarColumnas(df)

        df = transformarColumnas(df)

        df = df.select(orderFields.map(col): _*)
        df = applyDDLFromTableToDF(df,getObject((target_object).toString))
        writeWrapper(sourceDF = df, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)
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

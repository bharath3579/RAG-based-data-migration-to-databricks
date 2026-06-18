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


import org.apache.spark.sql.functions._
def readDfFromTable(tableName: String) : DataFrame = {
    val full_table_name = getObject(s"datahub01%env%aucdatagold.eyp_operational.${tableName}")
    val sourceMap = Map(
        "source_object" -> s"${full_table_name}",
        "source_format" -> "table",
        "source_alias" -> s"${tableName}"
    )
    var df = getDataFromSource(sourceMap)
    return df
}

val selectFieldsSQ = Seq("fec_process", "cod_vers_well_hole", "cod_vers_wellbore", "des_business_unit", "des_source_system", "des_business_term", 
"des_functional_dataset", "des_county", "des_pad", "des_category", "des_object_name", "des_source_dataset", "des_source_attribute", "des_dataset", "des_attribute", "val_value",
"ind_warning", "fec_drilling_end", "fec_completion_end", "fec_start_date")

def selectFields (df: DataFrame): DataFrame = {

    var dfTrn: DataFrame = df

    val selectFields = selectFieldsSQ 

    dfTrn = dfTrn.withColumnRenamed("CATEGORY", "des_category")
        .withColumnRenamed("attribute", "des_attribute")
        .withColumnRenamed("value", "val_value") 
        .withColumnRenamed("table_name", "des_dataset") 
        .withColumnRenamed("des_installation_name", "des_pad") 
        .withColumnRenamed("des_productunit", "des_business_unit") 
        .withColumnRenamed("SOURCE_SYSTEM", "des_source_system")
        .withColumnRenamed("BUSINESS_TERM", "des_business_term")
        .withColumnRenamed("FUNCTIONAL_DATASET_NAME", "des_functional_dataset")
        .withColumnRenamed("SOURCE_ATTRIBUTE_NAME", "des_source_attribute")
        .withColumnRenamed("SOURCE_DATASET_NAME", "des_source_dataset")

    dfTrn = dfTrn.select(selectFields.map(col): _*)

    return dfTrn

}

def toTranspose(df: DataFrame, columnsFixed: Seq[String], columnsTransposed: Seq[String]): DataFrame = {
    try {

        val transposeDf = df.select(
            (columnsFixed.map(col) ++
            Seq(expr(s"stack(${columnsTransposed.length}, " + 
            columnsTransposed.map(colName => s"'$colName', $colName").mkString(", ") + ") as (attribute, value)"))): _*
        ).select((columnsFixed.map(col) ++ Seq(col("attribute"), col("value"))): _*)

        println("Transposición realizada con éxito.")
        transposeDf.printSchema() 
        return transposeDf
    } catch {
        case e: Exception =>
            println(s"Error en la transposición: ${e.getMessage}")
            throw e  
    }
}
/*
def castColumnsToString(df: DataFrame, columns: Seq[String]): DataFrame = {
    columns.foldLeft(df) { (tempDf, colName) =>
        tempDf.withColumn(colName, col(colName).cast("string"))
    }
}*/

def castColumnsToString(df: DataFrame, columnsToCast: Seq[String]): DataFrame = {
    columnsToCast.foldLeft(df) { (tempDf, colName) =>
        if (tempDf.columns.contains(colName)) {
            tempDf.withColumn(colName, col(colName).cast("string"))
        } else {
            tempDf
        }
    }
}


// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------


def addVersionColumns(df: DataFrame): DataFrame = {
  // Definir la columna cod_vers_well_string
  val codVersWellhole = 
    if (df.columns.contains("id_vers_well_hole") && df.columns.contains("id_vers")){
        col("id_vers_well_hole").alias("cod_vers_well_hole")
    } else {
        col("id_vers").alias("cod_vers_well_hole")
    }

  val codVersWellbore = 
    if (df.columns.contains("id_vers_rel")){
        col("id_vers_rel").alias("cod_vers_wellbore")
    } else {
        lit(null).cast("string").alias("cod_vers_wellbore")
    }

  // Definir la columna cod_vers_well
  val fecDrillingend = 
    if (df.columns.contains("fec_date_td_reached")) {
      col("fec_date_td_reached").alias("fec_drilling_end")
    } else {
      to_timestamp(lit(null)).alias("fec_drilling_end")
    }

  val fecCompletionEnd = 
    if (df.columns.contains("fec_completed_end")) {
      col("fec_completed_end").alias("fec_completion_end")
    } else {
      to_timestamp(lit(null)).alias("fec_completion_end")
    }

  val deswellholebore = 
    if (df.columns.contains("des_well_hole")) {
      col("des_well_hole").alias("des_object_name")
    } else {
      col("des_well_bore").alias("des_object_name")
    }


  // Añadir las columnas al DataFrame
  df.withColumn("cod_vers_well_hole", codVersWellhole)
    .withColumn("fec_drilling_end", fecDrillingend)
    .withColumn("fec_completion_end", fecCompletionEnd)
    .withColumn("des_object_name", deswellholebore)
    .withColumn("cod_vers_wellbore", codVersWellbore)
}

// COMMAND ----------

def JoinsWithwellholeandHrchy(df: DataFrame, dfwell: DataFrame, dfhrchy: DataFrame): DataFrame = {
  // Seleccionar solo las columnas necesarias de dfwell
  val dfwellSelected = dfwell.select("id_vers", "des_county", "des_installation_name")
  
  // Primer join entre df y dfwellSelected
  val joinedWithWell = df.join(dfwellSelected, df("cod_vers_well_hole") === dfwellSelected("id_vers"), "left")
  
  // Seleccionar solo las columnas necesarias de dfhrchy
  val dfhrchySelected = dfhrchy.select("id_final", "des_productunit")
  
  // Segundo join entre el resultado anterior y dfhrchySelected
  val finalDf = joinedWithWell.join(dfhrchySelected, joinedWithWell("cod_vers_well_hole") === dfhrchySelected("id_final"), "left")

  finalDf 

    }

// COMMAND ----------


val dqFile = getDataFromSource(sourcesFinal("am_eyp0005_glb_attribute_for_qc_unconventionals")).filter(col("rule_type") === "COMPLETNESS")
val rangeTables: Seq[String] = dqFile.select("DATADRIVEN_DATASET_NAME").distinct.map(_.getString(0)).collect.toSeq

var dlInsert = "N/A"
var dlMsg = ""
var df: DataFrame  = null
var counter = 1
try {

    rangeTables.foreach { tableName =>
        println(s"Procesando tabla: $tableName")

        // Lee cada tabla y agrega el nombre de la tabla como columna adicional
        var dfTrn: DataFrame = readDfFromTable(tableName).withColumn("table_name", lit(tableName))
        // Llamar a la función para añadir las columnas de versión
        dfTrn = addVersionColumns(dfTrn)

        // Filtra las columnas que serán transpuestas
        val columnsToTranspose: Seq[String] = dfTrn.columns.filter(colName =>
            colName.startsWith("val_") || 
            colName.startsWith("cod_") ||
            colName.startsWith("des_") || 
            colName.startsWith("ind_") ||
            colName.startsWith("bol_") ||
            colName.startsWith("fec_") ||
            colName.startsWith("max_")
 
        )
        dfTrn = castColumnsToString(dfTrn, columnsToTranspose)
        val columnsFixed: Seq[String] = Seq("table_name", "cod_vers_well_hole", "cod_vers_wellbore", "fec_drilling_end", "fec_completion_end", "des_object_name", "fec_start_date")

        // Mensaje de depuración para ver las columnas seleccionadas
        println(s"Columnas a transponer: ${columnsToTranspose.mkString(", ")}")
        println(s"Columnas fijas: ${columnsFixed.mkString(", ")}")

        // Verifica si existen columnas para transponer
        dfTrn = toTranspose(dfTrn, columnsFixed, columnsToTranspose)

        // Une los DataFrames transpuestos o de solo columnas fijas
        if (df == null) {
            df = dfTrn
        } else {
            df = df.unionByName(dfTrn, allowMissingColumns = true)
        }
        }
    val dfJoin = dqFile.join(df, dqFile("DATADRIVEN_DATASET_NAME") === df("table_name") && dqFile("DATADRIVEN_ATTRIBUTE_NAME") === df("attribute"), "inner")

    val finalDf = dfJoin.withColumn("ind_warning",
                        when(col("value").isNotNull, lit(0)).    // 0 -> no nulo
                        otherwise(lit(1))                        // 1 -> nulo
                )
                .filter(col("ind_warning") === 1)
                .withColumn("fec_process", to_timestamp(date_format(current_timestamp(), "yyyy-MM-dd")))
    
    
    
    df = JoinsWithwellholeandHrchy(finalDf, getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_well_hole_unc")), getDataFromSource(sourcesFinal("eyp0007_tb_aux_d_dim_hrchy")))
    df = selectFields(df).withColumn("year", date_format(to_date(col("fec_process")), "yyyy"))
                        .withColumn("month", date_format(to_date(col("fec_process")), "MM"))
                        .withColumn("day", date_format(to_date(col("fec_process")), "dd"))

    val windowSpec = Window.partitionBy("des_object_name", "des_dataset", "des_attribute", "val_value", "fec_drilling_end", "fec_completion_end").orderBy(col("fec_start_date").desc)

    // Añadimos una columna "row_num" que indicará el número de cada fila en el orden de fec_start_date (descendente)
    val dfWithRowNumber = df.withColumn("row_num", row_number().over(windowSpec))

    // Filtramos para quedarnos solo con la primera fila (la más reciente) de cada grupo
    df  = dfWithRowNumber.filter(col("row_num") === 1).drop("row_num")
    df = df.withColumn("val_value", col("val_value").cast("Double")).withColumn("fec_drilling_end", to_timestamp(col("fec_drilling_end"))).withColumn("fec_completion_end", to_timestamp(col("fec_completion_end"))).withColumn("fec_start_date", to_timestamp(col("fec_start_date")))
    df = applyDDLFromTableToDF(df,getObject((target_object).toString))
    writeWrapper(sourceDF = df, conf = targetFinal, prefix="target", operationName = target_operation_name, optimize=optimizeDelta)
    dlInsert = "OK"

        } catch {
     case e: Throwable => 
                error = e.toString
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

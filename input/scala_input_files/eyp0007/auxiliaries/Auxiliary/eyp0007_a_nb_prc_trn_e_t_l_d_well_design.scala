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
// MAGIC  
// MAGIC  # Código que implementa la lógica necesaria

// COMMAND ----------

 
 import org.apache.spark.sql.expressions.Window

 def joinCasing (dfL: DataFrame, dfR: DataFrame): DataFrame = {

  var resDf = dfL.join(dfR, Seq("WELLBORE_ID"), "left")

  return resDf

  }

val finalSelectFields: Seq[String] = Seq("WELLBORE_ID", "des_casing_p_phase", "val_casing_p_hole_size", "val_casing_p_diameter", "val_casing_p_shoe_depth",
                                        "des_casing_1_phase", "val_casing_1_hole_size", "val_casing_1_diameter", "val_casing_1_shoe_depth",
                                        "des_casing_2_phase", "val_casing_2_hole_size", "val_casing_2_diameter", "val_casing_2_shoe_depth",
                                        "des_casing_3_phase", "val_casing_3_hole_size", "val_casing_3_diameter", "val_casing_3_shoe_depth", 
                                        "bol_flag_tubing")

 def cleanAssembly (df: DataFrame): DataFrame = {

    val windowAssembly = Window.partitionBy(col("WELLBORE_ID")).orderBy(col("ASSEMBLY_SIZE"))
    val windowAssemblyRowCount = Window.partitionBy(col("WELLBORE_ID"))
    var resDf = df.filter(col("CREATE_APP_ID") === "OpenWells" && col("ASSEMBLY_SIZE").isNotNull && col("STRING_TYPE") =!= "Tubing" && col("PHASE") === "ACTUAL" && col("MD_ASSEMBLY_BASE").isNotNull)
                .select("WELLBORE_ID", "ASSEMBLY_NAME", "HOLE_SIZE", "ASSEMBLY_SIZE", "MD_ASSEMBLY_BASE")
                .withColumn("casing_number", row_number.over(windowAssembly))
                .withColumn("count_rows", count("casing_number").over(windowAssemblyRowCount))
                .withColumn("lower_assembly_name", lower(col("ASSEMBLY_NAME")))
                .filter(col("count_rows") <= 4)

    return resDf
}

def getTubing (df: DataFrame): DataFrame = {

    var dfTubing = df.filter(col("STRING_TYPE") === "Tubing").select("WELLBORE_ID").withColumn("flag_tubing", lit(1)).dropDuplicates
    return dfTubing
}

// COMMAND ----------

var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {
    val dfAssembly: DataFrame = getDataFromSource(sourcesFinal("aa001236_glb_cd_assembly_t"))

    val dfAssemblyClean = cleanAssembly(dfAssembly)

    val dfTubing = getTubing(dfAssembly)

    val casings: Seq[String] = Seq("1", "p", "2", "3")

    casings.foreach(casing => {
        
        val mapFields: Map[String, String] = Map("ASSEMBLY_NAME" -> ("des_casing_"+casing+"_phase"), "HOLE_SIZE" -> ("val_casing_"+casing+"_hole_size")
                                ,"ASSEMBLY_SIZE" -> ("val_casing_"+casing+"_diameter"), "MD_ASSEMBLY_BASE" -> ("val_casing_"+casing+"_shoe_depth"))

        val selectFields: Seq[String] = mapFields.values.mkString(",").split(",") :+ "WELLBORE_ID"

        var dfAux: DataFrame = null

        casing match {
            case "1" => { dfAux = dfAssemblyClean.filter(col("casing_number") ===  col("count_rows") && !col("lower_assembly_name").contains("prod"))
                          dfAux = transformationRenameColumns(dfAux, mapFields).select(selectFields.map(col): _*)}
                                
            case "p" => { dfAux = dfAssemblyClean.filter(col("casing_number") ===  1 && col("lower_assembly_name").contains("prod"))
                          dfAux = transformationRenameColumns(dfAux, mapFields).select(selectFields.map(col): _*)}

            case "2" => { dfAux = dfAssemblyClean.filter(col("casing_number") ===  col("count_rows") - 1 && !col("lower_assembly_name").contains("prod"))
                          dfAux = transformationRenameColumns(dfAux, mapFields).select(selectFields.map(col): _*)}

            case "3" => { dfAux = dfAssemblyClean.filter(col("casing_number") ===  col("count_rows") - 2 && !col("lower_assembly_name").contains("prod"))
                          dfAux = transformationRenameColumns(dfAux, mapFields).select(selectFields.map(col): _*)}

            case _   => dfAux = null 
        }

        if (df == null) {
            df = dfAux
        }else {
            df = joinCasing(df, dfAux)
        }
    })

    df = joinCasing(df, dfTubing).withColumn("bol_flag_tubing", coalesce(col("flag_tubing"), lit(0)))

    df = df.select(finalSelectFields.map(col): _*)

    val dfFormatted = applyDDLFromTableToDF(df,getObject(targetFinal("target_object").toString))
    writeWrapper(sourceDF=dfFormatted,conf=targetFinal,prefix="target",operationName = target_operation_name,optimize=optimizeDelta)
    dlInsert = "OK"
    dlMsg = "Se han insertado "+df.count+ " registros"

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

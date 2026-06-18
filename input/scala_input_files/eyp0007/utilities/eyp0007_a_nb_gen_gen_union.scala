// Databricks notebook source
// MAGIC %md
// MAGIC # Plantilla Notebook para desarrollo a medida del proyecto EYP0007
// MAGIC
// MAGIC
// MAGIC #### Uso
// MAGIC Notebook generado para unir varios datasources que comparten esquemas similares
// MAGIC
// MAGIC
// MAGIC #### Nota
// MAGIC

// COMMAND ----------

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

// COMMAND ----------

var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

println("*"*25 + "\nSTART UNION PROCESS\n" + "*"*25)

try {

    def toScalaMap(obj: Any): Map[String, String] = obj match {
        case m: java.util.Map[_, _] => m.asScala.toMap.map { case (k, v) => k.toString -> v.toString }
        case m: Map[_, _] => m.map { case (k, v) => k.toString -> v.toString }
    }
    
    def toScalaList(obj: Any): List[String] = obj match {
        case l: java.util.List[_] => l.asScala.toList.map(_.toString)
        case l: Seq[_] => l.map(_.toString).toList
    }
    val flagFullLoad = paramsFinal.getOrElse("full_load", "false").toString.toBoolean
    val daysProcess = auxParamsFinal.getOrElse("days_process", "0").toString.toInt
    val filterColumn = auxParamsFinal.getOrElse("filter_column", "").toString
    val excludeValues = auxParamsFinal.getOrElse("exclude_values", List()) match {
        case l: java.util.List[_] => l.asScala.toList.map(_.toString)
        case l: List[_] => l.map(_.toString)
        case _ => List.empty[String]
    }
    val filterDateCols = auxParamsFinal.getOrElse("filter_date_cols", List.empty[String]) match {
        case l: java.util.List[_] => l.asScala.toList.map(_.toString)
        case l: List[_] => l.map(_.toString)
        case _ => List.empty[String]
    }


    sourcesFinal.foreach { case (_, x) =>

        val sourcePath = x.getOrElse("source_object", "ERROR source_object").toString
        val regexBu = """(?i)eckernel_([^-\s`]+)""".r

        val buValue: String = getSubstringByRegex(sourcePath, regexBu) match {
            case Some(bu) => bu
            case None => "ZZZ"
        }

        var dfTrn: DataFrame = null

        if (!flagFullLoad && daysProcess > 0) {

            val fieldsFilter =
               auxParamsFinal.get("fieldsFilter").map(toScalaList).getOrElse(List("created_date", "last_updated_date"))

            var referenceDateAux =
                if (referenceDate != null) referenceDate else ""

            if (referenceDateAux.isEmpty) {
                referenceDateAux = returnDateNowString()
            }

            val filterWhere =
                getWhereFilterDays(referenceDateAux, fieldsFilter, daysProcess)

            dfTrn =
                getDataFromSource(x, filterWhere)
                .withColumn("id_bu", lit(buValue.toUpperCase))

        } else {

            dfTrn =
                getDataFromSource(x)
                .withColumn("id_bu", lit(buValue.toUpperCase))
        }
        print(dfTrn.count())
        if (df == null) df = dfTrn
        else df = df.unionByName(dfTrn, allowMissingColumns = true)
    }


    if (filterColumn.nonEmpty && excludeValues.nonEmpty) {
        print("---> eliminando registros: " + excludeValues)
        df = df.filter(!col(filterColumn).isin(excludeValues: _*))
    }
    if (filterDateCols.nonEmpty) {
    val min = "1900-01-01 00:00:00"
    val max = "2079-06-06 23:59:59"

    val existingCols = filterDateCols.filter(df.columns.contains)

    existingCols.foreach { c =>
        print(s"---> Filtering values out-of-range in col: $c")

        val removedRows = df.filter(col(c).isNotNull && !(col(c).between(min, max)))
        
        df = df.filter(col(c).isNull || col(c).between(min, max))
    }
    }


    
    val typeProcess = auxParamsFinal.getOrElse("type_process", "NORMAL").toString
    if (typeProcess.toUpperCase == "DIMENSION") {
        val idVersFields = auxParamsFinal.get("id_vers_fields").map(toScalaMap).getOrElse(Map("daytime" -> "object_id"))
        df = addFieldIdVers(df, idVersFields)
    }
    val mapFields = auxParamsFinal.get("map_fields").map(toScalaMap).getOrElse(Map.empty[String, String])
    df = transformationRenameColumns(df, mapFields)
    
    if (typeProcess.toUpperCase == "DIMENSION") {
        df = addRowDummy(df)
    }
    
    val selectFields = auxParamsFinal.get("select_fields").map(toScalaList).getOrElse(List.empty[String])
    if (selectFields.nonEmpty) df = selectFieldsDataframe(df, selectFields)
    
    val castFields = auxParamsFinal.get("cast_fields").map(toScalaMap).getOrElse(Map.empty[String, String])
    if (castFields.nonEmpty) df = castDataFrame(df, castFields)
    val insertFields = auxParamsFinal.get("insert_fields").map(toScalaMap).getOrElse(Map.empty[String, String])
    val finalDf = addMissingColumns(df, insertFields)
    finalDf.printSchema()
    val dfFormatted = applyDDLFromTableToDF(finalDf,getObject((target_object).toString))
    writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

    val countWrites = df.count()
    println(s"Process complete with: $countWrites united rows")
} catch {
    case e: Throwable => 
                error = e.toString
                println(error)
                LogHelper().logEndKO(error)
}

println("*"*25 + "\nEND UNION PROCESS\n" + "*"*25)

// COMMAND ----------

val result = s"""{"error": "$error","dlInsert": { "result": "$dlInsert", "msg": "$dlMsg" } }"""

if (!error.isEmpty || dlInsert.equals("KO")) {
    LogHelper().logError(s"$name: $result")
    throw new Exception(s"$name: $result")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

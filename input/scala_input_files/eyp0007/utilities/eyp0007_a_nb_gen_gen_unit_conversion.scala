// Databricks notebook source
// MAGIC %md
// MAGIC # Plantilla Notebook para desarrollo a medida del proyecto EYP0007
// MAGIC
// MAGIC
// MAGIC #### Uso
// MAGIC Notebook generado para realizar tranformaciones asociadas al unit conversion
// MAGIC
// MAGIC
// MAGIC #### Nota

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



var df: DataFrame  = null

println("*"*25 + "\nSTART UNIT CONVERSION PROCESS\n" + "*"*25)

try {


    val dfFact = getDataFromSource(sourcesFinal("table_1")).withColumnRenamed("id_bu", "id_bu_fac")
                                                            .withColumn("id_bu_fac_aux", when(col("id_bu_fac") === "ARG", "1")
                                                                                        .when(col("id_bu_fac") === "BRA", "2")
                                                                                        .when(col("id_bu_fac") === "RNAS", "3")
                                                                                        .otherwise("4"))

    val className = auxParamsFinal.getOrElse("class_name", "ERROR class_name").toString
    val dfClassAttrPresentation = getDataFromSource(sourcesFinal("table_2")).filter(s"class_name = '$className'").withColumnRenamed("id_bu", "id_bu_class_attr")
   /* if (dfClassAttrPresentation.count == 0) {throw new Exception(s"$name: No se ha encontrado ninguno registro para el class name: $className")}*/


    val dfUomSetup = getDataFromSource(sourcesFinal("table_3")).withColumnRenamed("id_bu", "id_bu_uom")
                                                                .withColumn("id_bu_uom_aux", when(col("id_bu_uom") === "ARG", "1")
                                                                                            .when(col("id_bu_uom") === "BRA", "2")
                                                                                            .when(col("id_bu_uom") === "RNAS", "3")
                                                                                            .otherwise("4"))
                                                            .select("db_unit", "measurement_type", "id_bu_uom_aux", "unit").distinct 
    val dfUomSetupFull = getDataFromSource(sourcesFinal("table_3")).withColumnRenamed("id_bu", "id_bu_uom")

    val dfUnitConversion = getDataFromSource(sourcesFinal("table_4")).withColumnRenamed("id_bu", "id_bu_unit_conversion")

  
    val countReads = dfFact.count()
    println(s"Process init with: $countReads rows read base")

    val fieldsFixed =
        auxParamsFinal("fields_fixed")
        .asInstanceOf[List[_]]
        .map(_.toString) ++ Seq("id_bu_fac", "id_bu_fac_aux")

        val fieldsTransposed =
        auxParamsFinal("fields_transposed")
        .asInstanceOf[List[_]]
        .map(_.toString)



    df = toTransposeDataframe(dfFact, fieldsFixed, fieldsTransposed)

    df = joinDataframes(df, dfClassAttrPresentation, Map("attribute" -> "attribute_name", "id_bu_fac" -> "id_bu_class_attr"), Seq("uom"), "left")

    //Get Db unit
    df = joinDataframes(df, dfUomSetup.filter("db_unit = 'Y'"), Map("uom" -> "measurement_type", "id_bu_fac_aux" -> "id_bu_uom_aux"), Seq("unit"), "left")
    df = df.withColumnRenamed("unit", "db_unit_aux")

    //Get View Units
    val dfUomSetupWithAttr = joinDataframes(dfClassAttrPresentation, dfUomSetupFull, Map("uom" -> "measurement_type"), Seq("db_unit", "view_unit", "unit"), "inner")
                            .filter("db_unit == 'Y' OR view_unit == 'Y'")
                            .select("class_name", "attribute_name", "unit").distinct
                            .withColumnRenamed("unit", "unit_aux")

    df = joinDataframes(df, dfUomSetupWithAttr, Map("attribute" -> "attribute_name"), Seq("unit_aux"), "inner")
    df = df.withColumnRenamed("db_unit_aux", "db_unit").withColumnRenamed("unit_aux", "unit")
    

    df = joinDataframes(df, dfUnitConversion.select("from_unit","to_unit", "mult_fact","add_numb").distinct, Map("db_unit" -> "from_unit",  "unit" -> "to_unit"), Seq("mult_fact", "add_numb"), "left")
    df = df.withColumn("mult_fact", when(col("db_unit").equalTo(col("unit"))  && col("mult_fact").isNull, lit(1)).otherwise(col("mult_fact")))
                                                .withColumn("add_numb", when(col("db_unit").equalTo(col("unit"))  && col("add_numb").isNull, lit(0)).otherwise(col("add_numb")))
    df = df.withColumn("value_final", (col("value") * col("mult_fact")) + col("add_numb"))

    df = toPivotDataframe(df,fieldsFixed)

    val dfFormatted = applyDDLFromTableToDF(df,getObject((target_object).toString))
    writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

    val countWrites = df.count()
    println(s"Process complete with: $countWrites rows writes")

    if (countReads != countWrites) {println(s"WARNING: Base rows read are different to rows write.")}
   
} catch {
    case e: Throwable => 
                error = e.toString
                println(error)
                LogHelper().logEndKO(error)
}

println("*"*25 + "\nEND UNIT CONVERSION PROCESS\n" + "*"*25)

// COMMAND ----------


val result = s"""{"error": "$error"}"""

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

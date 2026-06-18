// Databricks notebook source
// MAGIC %md
// MAGIC # Notebook para control de calidad de entrada de un dataset
// MAGIC
// MAGIC #### Objetivo
// MAGIC Fijar las precondiciones de ejecución de una pipeline
// MAGIC
// MAGIC #### Uso
// MAGIC Para su hay que definir los siguiente parámetros desde la pipeline que se invoque
// MAGIC
// MAGIC | Parámetro | Valor por defecto | Descripcion |
// MAGIC |---|---|---|
// MAGIC |applicationName||Nombre de la aplicación|
// MAGIC |uuid||Identificador único de la ejecución|
// MAGIC |puid||Identificador único del padre|
// MAGIC |digitalCase||Identificador del caso digital|
// MAGIC |sources||<table class="GeneratedTable">  <thead>    <tr>      <th>Parámetro</th>      <th>Descripción</th>    </tr>  </thead>  <tbody>    <tr>          </tr>    <tr>         </tr>    <tr>      <td>source_object</td>      <td>Ruta en la que se encuentran los datos</td>    </tr>    <tr>      <td>source_format</td>      <td>Formato del objeto a leer</td>    </tr>  <tr>      <td>source_format_options</td>      <td>Opciones del formato</td>    </tr> <tr>      <td>source_alias</td>      <td>Identificador del dataset</td>    </tr> </tbody></table>|
// MAGIC |params||Parámetros de la pipeline|
// MAGIC |aux_params||Parámetros auxiliares|
// MAGIC |checks||Comprobaciones a realizar|
// MAGIC |name||Nombre para el trazado|
// MAGIC |spark_properties||Propiedades para cambiar en la session de spark|
// MAGIC
// MAGIC
// MAGIC #### Nota
// MAGIC
// MAGIC Las opciones de chequeo soportadas son las siguientes:
// MAGIC 1. NotNull
// MAGIC 1. Unique
// MAGIC 1. ValueIn
// MAGIC 1. CheckLastUpdated
// MAGIC

// COMMAND ----------

// DBTITLE 1,modified by ct team
dbutils.widgets.text("uuid", "N/A")
val uuid = dbutils.widgets.get("uuid")

dbutils.widgets.text("puid", "N/A")
val puid = dbutils.widgets.get("puid")


dbutils.widgets.text("params", "{}")
val params = dbutils.widgets.get("params")

dbutils.widgets.text("auxParams", """{}""")
val auxParams = dbutils.widgets.get("auxParams")

dbutils.widgets.text("sources", """[]""")
val sources = dbutils.widgets.get("sources")

dbutils.widgets.text("checks", "[]")
val checks = dbutils.widgets.get("checks")

dbutils.widgets.text("name", "")
val name = dbutils.widgets.get("name")

dbutils.widgets.text("sparkProperties", """{}""")
val sparkProperties = dbutils.widgets.get("sparkProperties")

dbutils.widgets.text("digitalCase", "None")
val digitalCase = dbutils.widgets.get("digitalCase")

dbutils.widgets.text("applicationName", "None")
val applicationName = dbutils.widgets.get("applicationName")


// COMMAND ----------

// MAGIC %md
// MAGIC ### Defining Helper functions

// COMMAND ----------

// MAGIC %run /Shared/DAB-PAN0001/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_a_nb_arquetipo_functions

// COMMAND ----------

implicit val spark1:SparkSession=spark

// COMMAND ----------

import com.repsol.datalake.log._

implicit val uid_app = (uuid, puid, applicationName)

// COMMAND ----------

import java.time.{LocalDateTime}
import java.time.format.{DateTimeFormatter}
 
// calculate time in advance in case something weird happens
val currentTime = LocalDateTime.now()
val dateFormater = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

// COMMAND ----------

// Parse the JSON string 'sources' into a List of Maps
val sourcesFinal: List[Map[String, Any]] = mapper.readValue[List[Map[String, Any]]](sources)

// Parse the JSON string 'sparkProperties' into a Map
val sparkPropertiesFinal: Map[String, String] = mapper.readValue[Map[String, String]](if (sparkProperties.trim.isEmpty) "{}" else sparkProperties)

// Parse the JSON string 'checks' into a List of Maps
val checksFinal: List[Map[String, Any]] = mapper.readValue[List[Map[String, Any]]](checks)

// Parse the JSON string 'params' into a Map
val paramsFinal: Map[String, String] = mapper.readValue[Map[String, String]](params)

// Set each Spark configuration property from the parsed 'sparkPropertiesFinal' Map
sparkPropertiesFinal.foreach(item => {
    println(s"Setting spark property ${item._1} to ${item._2}")
    spark.conf.set(item._1, item._2)
})

// Implicitly define 'myDigitalCase' with the value of 'digitalCase'
implicit val myDigitalCase = digitalCase

// COMMAND ----------

// Calculate 'adjustedReferenceDate' based on the presence of 'year' in 'paramsFinal'
val adjustedReferenceDate = if (paramsFinal.getOrElse("year", "KO").equals("KO")) {
    // If 'year' is not present, use the current time
    currentTime 
} else {
    // If 'year' is present, construct a LocalDateTime from 'year', 'month', and 'day' in 'paramsFinal'
    LocalDateTime.parse(s"${paramsFinal.getOrElse("year", "1970")}-${paramsFinal.getOrElse("month", "01")}-${paramsFinal.getOrElse("day", "01")}T00:00:00.00")
}

// Print various variables for debugging purposes
println(s"uuid = $uuid")
println(s"puid = $puid")
println(s"sources = $sources")
println(s"checks = $checks")
println(s"name = $name")
println(s"params = $params")
println(s"digitalCase = $digitalCase")
println(s"adjustedReferenceDate = $adjustedReferenceDate")

// COMMAND ----------


// Configuraciones de envio y Test a realizar 
println(s"params = $auxParams")

// COMMAND ----------

import org.apache.spark.sql.{Dataset, Row}

// Function to create a list of cached datasets from the sourcesFinal list
def datasetsMap: List[Dataset[Row]] = {
    // Map over each source data in sourcesFinal, get the corresponding dataset and cache it
    sourcesFinal.map(sdata => getDataFromSource(sdata).cache)
}

// COMMAND ----------

// DBTITLE 1,CT Team :deequ needs to be added as a library to the cluster
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus

// Function to generate checks based on the provided map configuration
def getCheck(map:Map[String,Any]) = {
    // Extract description from the map, default to "n/a" if not present
    val desc: String = map.getOrElse("desc", "n/a").asInstanceOf[String]
    // Extract columns from the map, default to List("n/a") if not present
    val columns: List[String] = map.getOrElse("columns", List("n/a")).asInstanceOf[List[String]]
    // Extract options from the map, default to an empty Map if not present
    val options:Map[String, Any] = map.getOrElse("options", Map()).asInstanceOf[Map[String, Any]]
    // Match the check type and generate corresponding checks
    map.getOrElse("check_type", "n/a") match {
        // Check for NotNull constraint
        case "NotNull" => columns.map(column => Check(CheckLevel.Error, 
                                                       s"Type: hasCompleteness, Column: $column, description: $desc").
                                                hasCompleteness(column, _ == 1.0))

        // Check for Unique constraint
        case "Unique" => List(Check(CheckLevel.Error, 
                              s"Type: hasUniqueness, Column: $columns, description: $desc").
                         hasUniqueness(columns, _ == 1.0))

        // Check for ValueIn constraint
        case "ValueIn" => {
            // Extract values from options
            val values = options.getOrElse("values", List()).asInstanceOf[List[String]]
            columns.map(column => Check(CheckLevel.Error, 
                                                       s"Type: isContainedIn, Column: $column, values: $values, description: $desc").
                                                isContainedIn(column, values.toArray))
        }
        
               
        // Check for LastUpdated constraint
        case "CheckLastUpdated" => {
            // Extract format and atLeastSinceDays from options
            val format = options.getOrElse("format", "yyyy-MM-dd").asInstanceOf[String]
            val atLeastSinceDays = options.getOrElse("atLeastSinceDays", -1).asInstanceOf[Int]
            // Create expression to validate last update format
            val expresion = s"TO_DATE(CONCAT_WS('-', ${columns.mkString(""",""")}), '$format') > CAST('${adjustedReferenceDate.minusDays(atLeastSinceDays).format(dateFormater)}' AS TIMESTAMP)"
            List(Check(CheckLevel.Error, desc).satisfies(expresion, "ValidateLastUpdateFormat", _>0))
        }

        // Default case for unknown check types
        case _ => List(Check(CheckLevel.Error, desc))

    }
}

// COMMAND ----------

// Generate a list of checks by applying the getCheck function to each element in checksFinal
val checks = checksFinal.flatMap(getCheck)

// Map over each source in sourcesFinal to generate verification results
val checksResults = sourcesFinal.map(ds => {
    // Retrieve the dataset for the current source
    val dataset = getDataFromSource(ds)
    // display(dataset)
    // Run the verification suite on the dataset with the generated checks
    (ds.getOrElse("source_alias", "n/a").asInstanceOf[String], VerificationSuite()
                    .onData(dataset)
                    .addChecks(checks)
                    .run())
})

// COMMAND ----------

// MAGIC %md
// MAGIC Parse Check Results

// COMMAND ----------

import com.amazon.deequ.VerificationResult
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}

// Convert the verification results to DataFrames and add a column for the source alias
val dfs = checksResults.map(x => {  
   VerificationResult.checkResultsAsDataFrame(spark, x._2).withColumn("id", lit(x._1))
}).reduce(_ unionByName _)

// Display the DataFrame sorted by check status
display(dfs.sort(asc("check_status")))

// COMMAND ----------

// MAGIC %md
// MAGIC Informar del resultado

// COMMAND ----------

// Iterate over each element in checksResults
checksResults.foreach(x => 
  // Log the verification results as JSON for each source
  LogHelper().logInfo(s"$name: ${VerificationResult.checkResultsAsJson(x._2)}")
)

// COMMAND ----------

// DBTITLE 1,changed by CT team
// Filter the DataFrame to get only the rows where the check status is 'Error'
val errors = dfs.filter("check_status = 'Error'").select("constraint", "constraint_message")

// Check if there are any errors
if (errors.take(1).size > 0) {
   // Convert the errors to a JSON-like string format
   val txt = "[\n" + errors.collect.map( x => s"""  {\n    "constraint":"${x.getString(0)}"\n    "constraint_message":"${x.getString(1)}"\n  }""").mkString(",\n") + "\n]"
   
   // Log the errors
   LogHelper().logError(s"$name: $txt")
   
   // Throw an exception with the error details
   throw new Exception(txt)
} else {
    // Exit the notebook with a success message if there are no errors
    dbutils.notebook.exit(s"""{"result": "SUCCESS"}""")
}

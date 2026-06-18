// Databricks notebook source
// MAGIC %md
// MAGIC # Notebook de carga en Data lake
// MAGIC
// MAGIC #### Objetivo
// MAGIC Este notebook una herramienta generica para la carga de ficheros en un datalake
// MAGIC
// MAGIC #### Uso
// MAGIC Para su hay que definir los siguiente parámetros desde la pipeline que se invoque
// MAGIC
// MAGIC | Parámetro | Valor por defecto | Descripcion |
// MAGIC |---|---|---|
// MAGIC |applicationName||Nombre de la aplicación|
// MAGIC |uuid||Identificador único de la ejecución|
// MAGIC |puid||Identificador único del padre|
// MAGIC |sources||<table class="GeneratedTable">  <thead>    <tr>      <th>Parámetro</th>      <th>Descripción</th>    </tr>  </thead>  <tbody>    <tr>          </tr>    <tr>         </tr>    <tr>      <td>source_object</td>      <td>Ruta en la que se encuentran los datos</td>    </tr>    <tr>      <td>source_format</td>      <td>Formato de la ruta a leer</td>    </tr>  <tr>      <td>source_format_options</td>      <td>Opciones del formato</td>    </tr>       <tr>      <td>source_deep_partition</td>      <td>Indica la profundidad de particiones a analizar (0 lo desactiva)</td>    </tr>  <tr>      <td>source_num_partitions</td>      <td>Carga el numero de particiones que se indica (0 lo desactiva)</td>    </tr>   <tr>      <td>source_alias</td>      <td>Nombre para eferenciar este origen desde SQL</td>    </tr>           </tbody></table>|
// MAGIC |sql||Consulta que se ejecutará sobre los datos cargados|
// MAGIC |target||<table class="GeneratedTable">  <thead>    <tr>      <th>Parámetro</th>      <th>Descripción</th>    </tr>  </thead>  <tbody>    <tr>          </tr>    <tr>          </tr>    <tr>      <td>target_object</td>      <td>Ruta donde se almacenarán los datos resultantes</td>    </tr>    <tr>      <td>target_format</td>      <td>Formato de la ruta a escribir</td>    </tr>   <tr>      <td>target_mode</td>  <td>Modo de escritura (append, overwrite) </td>    </tr> <tr>      <td>target_overwrite_mode</td>  <td>Modo de sobreescritura (dynamic, static) </td>    </tr>  <tr>         </tr>     <tr>      <td>target_dl_name_tags</td>      <td>Campos por donde se particiona</td>    </tr>           </tbody></table>|
// MAGIC |optimizeDelta|false|Indica si se ejecuta _vacuum_ y _optimize_ en la tabla delta|
// MAGIC |photon||To enable pass value true else false|
// MAGIC |params||Mapa escrito en JSON con las sustituciones que hay que aplicar a la sentencia SQL|
// MAGIC |auxparams||Parámetros auxiliares|
// MAGIC |dropFields||Lista de campos que no se quieres escribir en el fichero destino|
// MAGIC |sparkProperties||Propiedades para cambiar en la session de spark|
// MAGIC |name||Nombre para el trazado|
// MAGIC |digitalCase||Identificador del caso digital|
// MAGIC
// MAGIC
// MAGIC #### Nota
// MAGIC
// MAGIC En las rutas en formato delta se permite evolución de esquema.
// MAGIC
// MAGIC

// COMMAND ----------

dbutils.widgets.text("applicationName", "")
val applicationName: String = dbutils.widgets.get("applicationName")

dbutils.widgets.text("uuid", "N/A")
val uuid: String = dbutils.widgets.get("uuid")

dbutils.widgets.text("parentUid", "N/A")
val puid: String = dbutils.widgets.get("parentUid")

dbutils.widgets.text("params", "{}")
val params: String = dbutils.widgets.get("params")

dbutils.widgets.text("auxParams", "{}")
val auxParams: String = dbutils.widgets.get("auxParams")

dbutils.widgets.text("dropFields", "[]")
val dropFields: String = dbutils.widgets.get("dropFields")

dbutils.widgets.text("sources", "")
val sources: String = dbutils.widgets.get("sources")

dbutils.widgets.text("sql", "")
val sql: String = dbutils.widgets.get("sql")

dbutils.widgets.text("sqlFile", "")
val sqlFile: String = dbutils.widgets.get("sqlFile")

dbutils.widgets.text("target", "")
val target: String = dbutils.widgets.get("target")

dbutils.widgets.text("optimizeDelta", "false")
val optimizeDelta: Boolean = dbutils.widgets.get("optimizeDelta").toBoolean
var error:String = ""

dbutils.widgets.text("name", "")
val name: String = dbutils.widgets.get("name")

dbutils.widgets.text("sparkProperties", """{}""")
val sparkProperties: String = dbutils.widgets.get("sparkProperties")

dbutils.widgets.text("digitalCase", "None")
val digitalCase: String = dbutils.widgets.get("digitalCase")


// COMMAND ----------

// MAGIC %md
// MAGIC ### Defining helper functions

// COMMAND ----------

// MAGIC %run /Shared/DAB-EYP0007/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_a_nb_arquetipo_functions

// COMMAND ----------

implicit val spark1:SparkSession=spark

// COMMAND ----------

// MAGIC %md
// MAGIC ### Initialize and Apply Parameters to Configuration Variables

// COMMAND ----------

// Initialize adjustedSources with the value of sources
var adjustedSources = sources

// Function to apply parameters to a given text
def applyParams (params: String, txt: String): String = {
  var finalTxt = txt
  try {
    if (!params.trim.isEmpty) {
      // Parse the params JSON string into a Map
      val pFinal: Map[String, String] = mapper.readValue[Map[String, String]](params)

      // Replace placeholders in the text with corresponding parameter values
      for ((k,v) <- pFinal){
            finalTxt = finalTxt.replaceAll(s"%%${k}%%", v)
      }
    }  
  } catch {
         case e: Throwable => println(s"ERROR applyParams(): ${e.getStackTrace.mkString("\n")}")

  }
  finalTxt  
}

// Apply parameters to adjustedSources
adjustedSources = applyParams(params, adjustedSources)

// Parse adjustedSources JSON string into a List of Maps
val sourcesFinal: List[Map[String, Any]] = mapper.readValue[List[Map[String, Any]]](adjustedSources)

// Parse target JSON string into a Map
val targetFinal: Map[String, Any] = mapper.readValue[Map[String, Any]](target)

// Parse dropFields JSON string into a List of Strings
val dropFieldsFinal: List[String] = mapper.readValue[List[String]](dropFields)

// Parse sparkProperties JSON string into a Map
val sparkPropertiesFinal: Map[String, String] = mapper.readValue[Map[String, String]](if (sparkProperties.trim.isEmpty) "{}" else sparkProperties)

// Set each Spark property from the parsed sparkPropertiesFinal Map
sparkPropertiesFinal.foreach(item => {
    println(s"Setting spark property ${item._1} to ${item._2}")
    spark.conf.set(item._1, item._2)
})

// Set implicit value for digitalCase
implicit val myDigitalCase = digitalCase

// COMMAND ----------

// Determine the final SQL query to execute
var finalSql = if (sqlFile.isEmpty) {
  sql }
else 
  {readFile(s"config_$digitalCase/config_files/$sqlFile")} // Read SQL from file if sqlFile is not empty

// Apply auxiliary parameters to the final SQL query
finalSql = applyParams(auxParams, finalSql)

// Apply main parameters to the final SQL query
finalSql = applyParams(params, finalSql)

// Print the final SQL query to be executed
println(s"Executed SQL: $finalSql")

// COMMAND ----------

import com.repsol.datalake.log._

implicit val uid_app = (uuid, puid, applicationName)

println(s"applicationName = $applicationName")
println(s"uuid = $uuid")
println(s"puid = $puid")
println(s"sources = $adjustedSources")
println(s"sql = $sql")
println(s"sqlFile = $sqlFile")
println(s"finalSql = $finalSql")
println(s"target = $target")
println(s"optimizeDelta = $optimizeDelta")
println(s"name = $name")
println(s"params = $params")
println(s"digitalCase = $digitalCase")

// COMMAND ----------

var error = ""
val continue = if ( sources.size > 0 && !(sql.isEmpty && sqlFile.isEmpty)) {
        true 
    } else { 
        error = "Parametros incorrectos"
        false
    }

// COMMAND ----------

def getData = {
    if (continue) {
        // Iterate over each source in sourcesFinal and fetch data from the source
        sourcesFinal.foreach(x => getDataFromSource(x))
        // Execute the final SQL query and return the resulting DataFrame wrapped in Some
        Some(spark.sql(finalSql))
    } else {
        // If continue is false, return None
        None
    }
}

// COMMAND ----------

// Initialize variables to track the insert status and message
var dlInsert = "N/A"
var dlMsg = ""

// Check if the process should continue based on previous conditions
if (continue) {
    try {
        // Attempt to get the data and write it to the target
        getData.map(df_final => {
            if (dropFieldsFinal.isEmpty){
                writeWrapper(sourceDF=df_final,conf=targetFinal,optimize=optimizeDelta,prefix="target",operationName = targetFinal("target_operation_name").toString)
            } else {
                writeWrapper(sourceDF=df_final.drop(dropFieldsFinal:_*),conf=targetFinal,optimize=optimizeDelta,prefix="target",operationName = targetFinal("target_operation_name").toString)
            }
           
            dlInsert = "OK"
        })
    } catch {
        case e: Throwable => 
      
            dlMsg = e.toString
            dlInsert = "KO"
            // Log the error message 
            LogHelper().logEndKO(dlMsg)
    }
    // Log the insert status and paths 
    LogHelper().logInfo(dlInsert, 
      dataSetIn = sourcesFinal.map(x => getObject(x.getOrElse("source_object", "ERROR source_object").toString)).toList, 
      dataSetOut = List(getObject(targetFinal.getOrElse("target_object", "ERROR target_object").toString)))
}

// Print the insert status and message
println(dlInsert)
println(dlMsg)

// COMMAND ----------

val result = s"""{"error": "$error", "result": "$dlInsert", "msg": "$dlMsg" }"""

if (!error.isEmpty || dlInsert.equals("KO")) {
    LogHelper().logError(s"$name: $result")
    throw new Exception(s"$name: $result")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

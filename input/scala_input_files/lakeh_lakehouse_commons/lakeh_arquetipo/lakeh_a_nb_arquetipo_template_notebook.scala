// Databricks notebook source
// MAGIC %md
// MAGIC # Plantilla Notebook para desarrollo a medida
// MAGIC
// MAGIC #### Objetivo
// MAGIC Este notebook una plantilla para implementar un desarrollo a medida
// MAGIC
// MAGIC #### Uso
// MAGIC Para su hay que definir los siguiente parámetros desde la pipeline que se invoque
// MAGIC
// MAGIC | Parámetro | Valor por defecto | Descripcion |
// MAGIC |---|---|---|
// MAGIC |applicationName||Nombre de la aplicación|
// MAGIC |uuid||Identificador único de la ejecución|
// MAGIC |puid||Identificador único del padre|
// MAGIC |uniqueKey| |Identificador de la ejecución|
// MAGIC |digitalCase| |Código del caso digital|
// MAGIC |sources||<table class="GeneratedTable">  <thead>    <tr>      <th>Parámetro</th>      <th>Descripción</th>    </tr>  </thead>  <tbody>           <tr>      <td>source_object</td>      <td>Ruta/table en la que se encuentran los datos</td>    </tr>    <tr>      <td>source_format</td>      <td>Formato de la ruta a leer</td>    </tr>  <tr>      <td>source_format_options</td>      <td>Opciones del formato</td>    </tr> <tr>      <td>source_alias</td>      <td>Identificador del dataset</td>    </tr> </tbody></table>|
// MAGIC |target||<table class="GeneratedTable">  <thead>    <tr>      <th>Parámetro</th>      <th>Descripción</th>    </tr>  </thead>  <tbody>   <tr>      <td>target_object     <td>Ruta/table donde se almacenarán los datos resultantes</td>    </tr>    <tr>      <td>target_format</td>      <td>Formato de la ruta a escribir</td>    </tr>  </tbody></table>|
// MAGIC |aux_params||Parámetros auxiliares|
// MAGIC |name||Nombre para el trazado|
// MAGIC |spark_properties||Propiedades para cambiar en la session de spark|
// MAGIC |referenceDate||Fecha de ejecución del orquestador|
// MAGIC
// MAGIC
// MAGIC #### Nota
// MAGIC
// MAGIC En las rutas en formato delta se permite evolución de esquema.
// MAGIC

// COMMAND ----------

dbutils.widgets.text("uuid", "N/A")
val uuid = dbutils.widgets.get("uuid")

dbutils.widgets.text("parentUid", "N/A")
val puid = dbutils.widgets.get("parentUid")

dbutils.widgets.text("uniqueKey", "")
val uniqueKey = dbutils.widgets.get("uniqueKey")

dbutils.widgets.text("digitalCase", "")
val digitalCase = dbutils.widgets.get("digitalCase")

dbutils.widgets.text("sources", "[{}]")
val sources = dbutils.widgets.get("sources")

dbutils.widgets.text("target", "{}")
val target = dbutils.widgets.get("target")

dbutils.widgets.text("targets", "[]")
val targets = dbutils.widgets.get("targets")

dbutils.widgets.text("name", "")
val name = dbutils.widgets.get("name")

dbutils.widgets.text("auxParams", "{}")
val auxParams = dbutils.widgets.get("auxParams")

dbutils.widgets.text("params", "{}")
val params = dbutils.widgets.get("params")

dbutils.widgets.text("sparkProperties", """{}""")
val sparkProperties = dbutils.widgets.get("sparkProperties")

dbutils.widgets.text("referenceDate", "")
val referenceDate = dbutils.widgets.get("referenceDate")

dbutils.widgets.text("applicationName", "")
val applicationName = dbutils.widgets.get("applicationName")

dbutils.widgets.text("optimizeDelta", "false")
val optimizeDelta: Boolean = dbutils.widgets.get("optimizeDelta").toBoolean
var error:String = ""

// COMMAND ----------

// MAGIC %run /Shared/DAB-PAN0001/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_a_nb_arquetipo_functions

// COMMAND ----------

// Deserialize the JSON string 'sources' into a List of Maps
val auxParamsFinal: Map[String, Any] = parseMapAny(auxParams)

// Deserialize the JSON string 'params' into a Map, defaulting to an empty JSON object if 'params' is empty
val paramsFinal: Map[String, Any] = parseMapAny(params)

val sparkPropertiesFinal: Map[String, String] = parseMapString(sparkProperties)

// Set each Spark configuration property from the 'sparkPropertiesFinal' Map
sparkPropertiesFinal.foreach(item => {
    println(s"Setting spark property ${item._1} to ${item._2}")
    spark.conf.set(item._1, item._2)
})

val targetsFinal = joinDatasets(dataset=target, datasets=targets, prefix="target")
val sourcesFinal = joinDatasets(datasets=sources, prefix="source") 


implicit val myDigitalCase = digitalCase

// COMMAND ----------

import com.repsol.datalake.log._
import com.repsol.datalake.control._

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
println(s"targets = $targets")
println(s"auxParams = $auxParams")
println(s"params = $params")
println(s"name = $name")

// COMMAND ----------

// MAGIC %md
// MAGIC # Código que implementa la lógica necesaria

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC # Informar del resultado

// COMMAND ----------

val result = "<cambiar por algo con sentido (mapa JSON sencillo)>"

if (!error.isEmpty) {
    LogHelper().logError(s"$name: $error")
    throw new Exception(s"$name: $error")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result) 

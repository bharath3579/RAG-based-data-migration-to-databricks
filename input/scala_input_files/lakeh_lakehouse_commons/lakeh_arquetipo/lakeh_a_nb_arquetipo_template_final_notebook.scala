// Databricks notebook source
// MAGIC %md
// MAGIC # Plantilla Notebook para desarrollo a medida
// MAGIC
// MAGIC #### Objetivo
// MAGIC Este notebook una plantilla para implementar un notebook de 
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
// MAGIC |sources||<table class="GeneratedTable">  <thead>    <tr>      <th>Parámetro</th>      <th>Descripción</th>    </tr>  </thead>  <tbody>     <tr>      <td>source_object</td>      <td>Ruta/table en la que se encuentran los datos</td>    </tr>    <tr>      <td>source_format</td>      <td>Formato de la ruta a leer</td>    </tr>  <tr>      <td>source_format_options</td>      <td>Opciones del formato</td>    </tr> <tr>      <td>source_alias</td>      <td>Identificador del dataset</td>    </tr> </tbody></table>|
// MAGIC |target||<table class="GeneratedTable">  <thead>    <tr>      <th>Parámetro</th>      <th>Descripción</th>    </tr>  </thead>  <tbody>      <tr>      <td>target_object</td>      <td>Ruta/table donde se almacenarán los datos resultantes</td>    </tr>    <tr>      <td>target_format</td>      <td>Formato de la ruta a escribir</td>    </tr>  </tbody></table>|
// MAGIC |aux_params||Parámetros auxiliares|
// MAGIC |params||Parámetros|
// MAGIC |name||Nombre para el trazado|
// MAGIC |spark_properties||Propiedades para cambiar en la session de spark|
// MAGIC |referenceDate||Fecha de ejecución del orquestador|
// MAGIC |error||Error de ejecución del arquetipo, en caso de ejecución KO|
// MAGIC |appName||applicationName para el envio de correos (ver documencatión: https://dev.azure.com/repsol-digital-team/Data%20Analytics%20Hub/_wiki/wikis/Data-Analytics-Hub.wiki/94969/5.14.7.2.2-Notebook)|
// MAGIC |receiver||destinatario_mail para el envio de correos (ver documentación: https://dev.azure.com/repsol-digital-team/Data%20Analytics%20Hub/_wiki/wikis/Data-Analytics-Hub.wiki/94969/5.14.7.2.2-Notebook)|
// MAGIC |subject||Asunto del mail|
// MAGIC |body||Cuerpo del mail|
// MAGIC

// COMMAND ----------

dbutils.widgets.text("applicationName", "")
val applicationName: String = dbutils.widgets.get("applicationName")

dbutils.widgets.text("uuid", "N/A")
val uuid: String = dbutils.widgets.get("uuid")

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

dbutils.widgets.text("error", "")
val error = dbutils.widgets.get("error")

dbutils.widgets.text("appName", "")
val appName = dbutils.widgets.get("appName")

dbutils.widgets.text("receiver", "")
val receiver = dbutils.widgets.get("receiver")

dbutils.widgets.text("subject", "")
val subject = dbutils.widgets.get("subject")

dbutils.widgets.text("body", "")
val body = dbutils.widgets.get("body")


// COMMAND ----------

/*
 val sources = s"""{
            "source_object":"datahub01%env%aucdatagold.sandbox_crp0025.employee_details",
             "source_format": "table",
             "source_deep_partition": 3,
             "source_num_partition": 2,
            "source_format_options": {}
            }"""

val target = s"""{
        "target_object":"datahub01%env%aucdatagold.sandbox_crp0025.emp_dt_2",
        "target_format": "table"
}
"""
*/

// COMMAND ----------

// MAGIC %md
// MAGIC ### Defining Helper Functions

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
import com.repsol.datalake.mail._

implicit val uid_app = (uuid, puid, applicationName)

// COMMAND ----------

// MAGIC %md
// MAGIC # Código que implementa la lógica necesaria

// COMMAND ----------

// DBTITLE 1,commented and modified by CT Team
SendMailHelper().sendMail(
  uuid.toString, 
  appName.toString, 
  receiver.toString, 
  subject.toString,
  s"""
  ${body.toString}.
  ERROR: ${error.toString}.
  """
)

// COMMAND ----------

// MAGIC %md
// MAGIC # Informar del resultado

// COMMAND ----------

dbutils.notebook.exit("fin")

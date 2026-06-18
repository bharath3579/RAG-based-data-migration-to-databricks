// Databricks notebook source
dbutils.widgets.text("uuid", "N/A")
val uuid = dbutils.widgets.get("uuid")

dbutils.widgets.text("parentUid", "N/A")
val puid = dbutils.widgets.get("parentUid")

dbutils.widgets.text("target", "")
val target = dbutils.widgets.get("target")

dbutils.widgets.text("targets", "")
val targets = dbutils.widgets.get("targets")

dbutils.widgets.text("targetCheck", "")
val targetCheck = dbutils.widgets.get("targetCheck")

dbutils.widgets.text("targetChecks", "")
val targetChecks = dbutils.widgets.get("targetChecks")

dbutils.widgets.text("name", "")
val name = dbutils.widgets.get("name")

dbutils.widgets.text("excludeFields", "[]")
val excludeFields = dbutils.widgets.get("excludeFields")

dbutils.widgets.text("removeTargetPath", "true")
val removeTargetPath = dbutils.widgets.get("removeTargetPath").toBoolean

dbutils.widgets.text("digitalCase", "None")
val digitalCase = dbutils.widgets.get("digitalCase")

dbutils.widgets.text("applicationName", "N/A")
val applicationName: String = dbutils.widgets.get("applicationName")


// COMMAND ----------

// MAGIC %md
// MAGIC ### Defining Helper Functions

// COMMAND ----------

// MAGIC %run /Shared/DAB-EYP0007/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_a_nb_arquetipo_functions

// COMMAND ----------

val excludeFieldsFinal: List[String] = mapper.readValue[List[String]](excludeFields)

val targetsFinal = joinDatasets(dataset=target, datasets=targets)
val targetsCheckFinal = joinDatasets(dataset=targetCheck, datasets=targetChecks)

implicit val myDigitalCase = digitalCase

// COMMAND ----------


implicit val uid_app = (uuid, puid, applicationName)

// LogHelper().logStart()

println(s"uuid = $uuid")
println(s"puid = $puid")

println(s"target = $target")
println(s"targetCheck = $targetCheck")

println(s"targets = $targets")
println(s"targetChecks = $targetChecks")

println(s"name = $name")
println(s"excludeFields = $excludeFields")

// COMMAND ----------

import scala.collection.mutable

val error: mutable.Map[String, String] = mutable.Map()
val continue = if ( targetsCheckFinal.size > 0 && targetsFinal.size > 0 && targetsCheckFinal.size == targetsFinal.size) {
        true 
    } else { 
        error("status") = s"Incorrect parameters: targetChecks: ${targetsFinal.size}, targetChecks: ${targetsCheckFinal.size}"
        false
    }

// COMMAND ----------

// MAGIC %md
// MAGIC # Checks
// MAGIC Columns names
// MAGIC
// MAGIC Contents

// COMMAND ----------

if (continue) {
    targetsFinal.keys.foreach( myAlias => {
        val targetFinal: Map[String,Any] = targetsFinal.get(myAlias) match {
            case Some(x) => x
            case None => Map()
        }
        val targetCheckFinal: Map[String,Any] = targetsCheckFinal.get(myAlias) match {
            case Some(x) => x
            case None => Map()
        }

        if ( targetFinal.isEmpty || targetCheckFinal.isEmpty) {
            println(s"Any dataset not found in $myAlias")
            error(myAlias) = s"Any dataset not found"
        } else {
            println(s"Comparing $myAlias: ${targetFinal.toString} - ${targetCheckFinal.toString}")

            // Retrieve data from source for targetFinal and targetCheckFinal
            val dfOrig = getDataFromSource(targetFinal, prefix = "target")
            val dfCheck = getDataFromSource(targetCheckFinal, prefix = "target")

            // Get and sort column names
            val colOrig: List[String] = dfOrig.columns.toList.sorted
            val colCheck: List[String] = dfCheck.columns.toList.sorted

            // Check if columns are the same
            if (colOrig.equals(colCheck)) {
                println("The columns are the same.")
            } else {
                error(myAlias) = s"The columns are different: [${colOrig.mkString(", ")}] <-> [${colCheck.mkString(", ")}]. "
                println(error)
            }

            // Filter out excluded fields from column lists
            var colOrigFinal = colOrig.filter(x => !excludeFieldsFinal.contains(x))
            val colCheckFinal = colCheck.filter(x => !excludeFieldsFinal.contains(x))

            // Select filtered columns from dataframes
            val dfOrigFinal = dfOrig.select(colOrigFinal.head, colOrigFinal.tail: _*)
            val dfCheckFinal = dfCheck.select(colCheckFinal.head, colCheckFinal.tail: _*)

            // Find differences between the two dataframes
            val dfDiff = dfOrigFinal.except(dfCheckFinal).withColumn("alias", lit(myAlias))
            var cont = dfDiff.count

            // Check if there are differences
            if (cont > 0) {
                println(s"----------------- $myAlias (Generated -> Check file) ---------------")
                error(myAlias) = s"The data is not the same, there are $cont different data. "
                println(s"The data is not the same, there are $cont different data. ")
                dfDiff.show()
                //dfOrigFinal.show()
                //dfCheckFinal.show()
            } else {
                println(s"----------------- $myAlias  ---------------")
                println("The data is the same (Generated -> Check file).")
                val dfDiff2 = dfCheckFinal.except(dfOrigFinal).withColumn("alias", lit(myAlias))
                cont = dfDiff2.count

                // Check again for differences in the opposite direction
                if (cont > 0) {
                    println(s"----------------- $myAlias (Check file -> Generated) ---------------")
                    error(myAlias) = s"The data is not the same, there are $cont different data. "
                    println(s"The data is not the same, there are $cont different data. ")
                    dfDiff2.show()
                    //dfOrigFinal.show()
                    //dfCheckFinal.show()
                } else {
                    println(s"----------------- $myAlias  ---------------")
                    println("The data is the same (Check file -> Generated).")
                }
            }
        }
    })
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Resultado de la operación

// COMMAND ----------

// unsure about the use of this functionality
if (removeTargetPath) {
  targetsFinal.values.foreach( targetFinal => {
    val targetObject = targetFinal("target_object").toString

    if (targetObject.startsWith("/Volumes/")) {
      dbutils.fs.rm(getObject(targetObject), true)
    }
  })
}


if (!error.isEmpty) {
    error.foreach( x => LogHelper().logError(s"${x._1}: ${x._2}"))
    throw new Exception(s"$name: ${error.toString}")
} 

LogHelper().logInfo(s"$name: Pass")
dbutils.notebook.exit(s"$name: Pass")  


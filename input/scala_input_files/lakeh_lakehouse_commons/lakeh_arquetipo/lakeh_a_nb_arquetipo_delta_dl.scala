// Databricks notebook source
// MAGIC %md
// MAGIC ## Data Lake and Data Warehouse Loading Notebook
// MAGIC ### Objective
// MAGIC This notebook is a generic tool for writing changes to a Delta table.
// MAGIC
// MAGIC ### Preconditions
// MAGIC The source dataset will be in Parquet format.
// MAGIC
// MAGIC The destination dataset will be in Delta format.

// COMMAND ----------

dbutils.widgets.text("uuid", "N/A")
val uuid: String = dbutils.widgets.get("uuid")

dbutils.widgets.text("parentUid", "N/A")
val puid: String = dbutils.widgets.get("parentUid")

dbutils.widgets.text("sources", "{}")
val sources: String = dbutils.widgets.get("sources")

dbutils.widgets.text("target", "{}")
val target: String = dbutils.widgets.get("target")

dbutils.widgets.text("optimizeDelta", "false")
val optimizeDelta: Boolean = dbutils.widgets.get("optimizeDelta").toBoolean
var error:String = ""

dbutils.widgets.text("keyFields", "[]")
val keyFields: String = dbutils.widgets.get("keyFields")

dbutils.widgets.text("dropFields", "[]")
val dropFields: String = dbutils.widgets.get("dropFields")

dbutils.widgets.text("orderField", "")
val orderField: String = dbutils.widgets.get("orderField")

dbutils.widgets.text("opField", "") 
val opField: String = dbutils.widgets.get("opField")

dbutils.widgets.text("ops", """{"INSERT": "C", "DELETE": "D", "UPDATE": "U"}""")
val ops: String = dbutils.widgets.get("ops")

dbutils.widgets.text("name", "")
val name: String = dbutils.widgets.get("name")

dbutils.widgets.text("params", "{}")
val params: String = dbutils.widgets.get("params")

dbutils.widgets.text("sparkProperties", """{}""")
val sparkProperties: String = dbutils.widgets.get("sparkProperties")

dbutils.widgets.text("autoMerge", "false")
val autoMerge: String = dbutils.widgets.get("autoMerge")

dbutils.widgets.text("digitalCase", "None")
val digitalCase: String = dbutils.widgets.get("digitalCase")

dbutils.widgets.text("applicationName", "N/A")
val applicationName: String = dbutils.widgets.get("applicationName")

dbutils.widgets.text("iterativeFlag", "false") 
val iterativeFlag = dbutils.widgets.get("iterativeFlag").toBoolean


// COMMAND ----------

// MAGIC %md
// MAGIC ### Defining common archetype functions

// COMMAND ----------

// MAGIC %run /Shared/DAB-PAN0001/files/src/lakeh_lakehouse_commons/lakeh_arquetipo/lakeh_a_nb_arquetipo_functions

// COMMAND ----------

implicit val spark1:SparkSession=spark

// COMMAND ----------

var adjustedSources = sources

// If params is not empty, replace placeholders in adjustedSources with actual values from params
if (!params.trim.isEmpty) {
      val pFinal: Map[String, String] = mapper.readValue[Map[String, String]](params)

      for ((k,v) <- pFinal){
            adjustedSources = adjustedSources.replaceAll(s"%%${k}%%", v)
      }
}

// Deserialize JSON strings into respective Scala collections
//val keyFieldsFinal: List[String] = mapper.readValue[List[String]](keyFields)
val dropFieldsFinal: List[String] = mapper.readValue[List[String]](dropFields)
// val opsFinal: Map[String, String] = mapper.readValue[Map[String, String]](ops)
val sourceFinal: Map[String, Any] = mapper.readValue[Map[String, Any]](adjustedSources)
val targetFinal: Map[String, Any] = mapper.readValue[Map[String, Any]](target)

// Deserialize sparkProperties JSON string into a Map, defaulting to an empty Map if sparkProperties is empty
val sparkPropertiesFinal: Map[String, String] = mapper.readValue[Map[String, String]](if (sparkProperties.trim.isEmpty) "{}" else sparkProperties)
sparkPropertiesFinal.foreach(item => {
    println(s"Setting spark property ${item._1} to ${item._2}")
    spark.conf.set(item._1, item._2)
})

// Enable autoMerge if specified
if (autoMerge.equalsIgnoreCase("true")) spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

implicit val myDigitalCase = digitalCase

// COMMAND ----------

// DBTITLE 1,need to uncomment this log import
import control.repsol.datalake.log._

implicit val uid_app = (uuid, puid, applicationName)

println(s"applicationName = $applicationName")
println(s"uuid = $uuid")
println(s"puid = $puid")
println(s"sources = $adjustedSources")
println(s"target = $target")
println(s"optimizeDelta = $optimizeDelta")
println(s"keyFieldsFinal = $keyFieldsFinal")
println(s"dropFieldsFinal = $dropFieldsFinal")
println(s"orderField = $orderField")
println(s"opField = $opField")
println(s"opsFinal = $opsFinal")
println(s"params = $params")
println(s"name = $name")
println(s"iterativeFlag = $iterativeFlag")

// COMMAND ----------

// DBTITLE 1,commented and modified by CT Team
val sFormatOptions: Map[String, String] = sourceFinal.getOrElse("source_format_options", Map.empty[String, String]).asInstanceOf[Map[String, String]]
val sFormat: String = sourceFinal.getOrElse("source_format", "ERROR source_format").toString
val numPartitionsRead: Int = sourceFinal.getOrElse("source_num_partition", "1").toString.toInt
val numDeepPartition: Int = sourceFinal.getOrElse("source_deep_partition", "3").toString.toInt
val sourceObject :String =sourceFinal.getOrElse("source_object", " ").toString
 val s_eObject = getObject(sourceObject)

val sfiltersTotal = if (sFormat.equalsIgnoreCase("table") && iterativeFlag==true) {
    getFilterConditionList(s_eObject, numDeepPartition, numPartitionsRead)
} 
val  sfiltersTotalStr = if (sFormat.equalsIgnoreCase("table") && iterativeFlag==false) {
    getFilterCondition(s_eObject, numDeepPartition, numPartitionsRead)
}

// COMMAND ----------

// DBTITLE 1,Deduplication of sk when iterative flag is false


import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

def deDuplicateSkOnPartFields(
  df: DataFrame,
  keyFieldsFinal: List[String],
  partitionFields: Seq[String] = Seq("year", "month", "day"),
  orderFields:List[String] 
): DataFrame = {

  // Helper: Escape column names if needed (e.g., special characters)
  def escape(colName: String): Column =
    if (colName.matches(".*[\\s\\-/|()<>].*")) col(s"`$colName`") else col(colName)

  // Partition by all keys
  val partitionCols = keyFieldsFinal.map(escape)

  // Order by partition fields descending
  val orderCols = (partitionFields ++ orderFields.toSeq).map(f => escape(f).desc)

  // Define window spec
  val winSpec = Window.partitionBy(partitionCols: _*).orderBy(orderCols: _*)

  // Apply deduplication
  df.withColumn("rnk", row_number().over(winSpec))
    .where(col("rnk") === 1)
    .drop("rnk")
}


// COMMAND ----------

def getSourceDataFrame(s_eObject:String ,filters : Option[String],sformat:String):DataFrame = {

    var sourceDfTmp = if (sformat.equalsIgnoreCase("table"))
        {
            spark.read.table(s_eObject).filter(filters.getOrElse("1=1"))
           
        }
        else {
            spark.read.format(sFormat).options(sFormatOptions).load(s_eObject).filter(filters.getOrElse("1=1"))
        }
    if (iterativeFlag==false)

    {
      val partitionColsfromSrc=sfiltersTotalStr.asInstanceOf[Option[(String,Seq[String])]].get._2.toSeq
     
   sourceDfTmp= deDuplicateSkOnPartFields(sourceDfTmp, keyFieldsFinal = keyFieldsFinal,partitionFields= partitionColsfromSrc,List(orderField))  

    }


    // Helper to escape column names with special characters
def escape(colName: String): Column =
  if (colName.matches(".*[\\s\\-/|()<>].*")) col(s"`$colName`") else col(colName)

// Step 1: Select only the keys and orderField
val changesDf = sourceDfTmp
  .select((keyFieldsFinal :+ orderField).map(escape): _*)
  .groupBy(keyFieldsFinal.map(escape): _*)
  .agg(max(orderField).as(orderField))

// Step 2: Create join condition on key fields and orderField
val joinCondition =
  keyFieldsFinal.map { colName =>
    sourceDfTmp(escape(colName).toString()) === changesDf(escape(colName).toString())
  }
  
  .reduce(_ && _) &&
  (sourceDfTmp(orderField) === changesDf(orderField))
  println(joinCondition)

// Step 3: Join to get final DF
val finalDf = sourceDfTmp
  .join(changesDf, joinCondition, "leftsemi")
  .drop(dropFieldsFinal: _*)

  finalDf  


}


// COMMAND ----------

sfiltersTotal.asInstanceOf[Option[(Seq[String],Seq[String])]].get._1

// COMMAND ----------

// DBTITLE 1,prepare list of source dataframes
val sourceDfs: List[DataFrame] = 
  if (!iterativeFlag) {
    List(getSourceDataFrame(s_eObject, Option(sfiltersTotalStr.asInstanceOf[Option[(String,Seq[String])]].get._1), sFormat))
  } else {
    sfiltersTotal.asInstanceOf[Option[(Seq[String],Seq[String])]].get._1.sortWith(_<_).toList.map(x => getSourceDataFrame(s_eObject, Option(x.toString), sFormat))
  }
  


// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading the Delta Table (Destination)

// COMMAND ----------

// DBTITLE 1,changed by ct team
import io.delta.tables._

var dlMsg = "" 
val tFormat:String = targetFinal.getOrElse("target_format", "table").toString   //asssuming if we have to use volume with delta format we will specify that in json 

val tDLNamePartitionsFinal:List[String] = targetFinal.getOrElse("target_dl_name_partitions", List()).asInstanceOf[List[String]] 
val targetObject :String =targetFinal.getOrElse("target_object", " ").toString
val t_eObject = getObject(targetObject) 

def getDeltaTable(): Option[DeltaTable] = {
    val isVolumePath = t_eObject.startsWith("/Volumes")

    val existsTable = if (isVolumePath) {
        DeltaTable.isDeltaTable(spark, t_eObject)
    } else {
        spark.catalog.tableExists(t_eObject)
    }

    var targetDf: Option[DeltaTable] = None

    if (existsTable) {
        try {
            targetDf = if (isVolumePath) {
                Some(DeltaTable.forPath(spark, t_eObject))
            } else {
                Some(DeltaTable.forName(spark, t_eObject))
            }
        } catch {
            case e: Throwable =>{
                dlMsg = e.toString
                LogHelper().logInfo(s"$t_eObject: $dlMsg")
                throw new Exception(dlMsg)}
            case _: Throwable => 
        }
    } 

    targetDf
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## We Apply Changes to the Table
// MAGIC The applied logic is as follows:
// MAGIC
// MAGIC If the key exists and matches, and it is a delete operation, the row is deleted.
// MAGIC
// MAGIC If the key exists and matches (and it is NOT a delete operation), the row is updated.
// MAGIC
// MAGIC If the key does not exist and it is NOT a delete operation, the row is inserted.
// MAGIC
// MAGIC Note: All columns from the DataFrame are updated and inserted, as this is an intermediate table.

// COMMAND ----------

// DBTITLE 1,changed code by CT team
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import io.delta.tables.DeltaTable

var dlInsert = "N/A" 
var error = "" 


import io.delta.tables._
import org.apache.spark.sql.DataFrame


// Helper: escape special characters in column names using backticks
  def escape(colName: String): String =
    if (colName.matches(".*[\\s\\-/|()<>].*")) s"`$colName`" else colName

try {
  val targetDfOpt = getDeltaTable() 
  sourceDfs.foreach( srcDf => {
    val targetDf = getDeltaTable().get.toDF
   targetDf.take(1).nonEmpty match {
  case false => {
    val dfAux = srcDf.filter(s"$opField = '${opsFinal("INSERT")}' OR $opField = '${opsFinal("UPDATE")}'")
    if (tFormat.equalsIgnoreCase("table")) {
      println("Count of src df: " + dfAux.count())
      writeWrapper(
        sourceDF = dfAux,
        conf = targetFinal,
        prefix = "target",
        operationName = "direct_write_using_writeModes"
      )
    }
    println("Creado nuevo")
  }
 
  case true => {
    println("Write wrapper execution for SCD with delete started...")
    writeWrapper(
      sourceDF = srcDf,
      targetTable = targetFinal.get("target_object").get.toString,
      joinCols = keyFieldsFinal,
      dropDeletes = true,
      whenMatchedExpr = s"source.${escape(opField)} = '${opsFinal("DELETE")}'",
      whenNotMatchedExpr = s"source.${escape(opField)} != '${opsFinal("DELETE")}'",
      opField = s"${escape(opField)}",
      operationName = targetFinal.get("target_operation_name").get.toString,
      optimize = false
    )
    println("Write wrapper execution for SCD with delete completed...")
    println("Actualizado")
  }
}
  } )
//optimizeDelta
if(optimizeDelta==true)
{
runLiquidMaintenance(
  tableName=targetFinal.get("target_object").get.toString
)
}


  dlInsert = "OK"
} catch {
    case e: Throwable => 
        dlMsg = e.toString
        dlInsert = "KO"
        LogHelper().logEndKO(dlMsg)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Result of the operation

// COMMAND ----------

val result = s"""{"error": "$error", "result": "$dlInsert", "msg": "$dlMsg" }"""
println(result)
if (dlInsert.equals("KO")) {
    LogHelper().logError(s"$name: $result")
    throw new Exception(s"$name: $result")
} 

LogHelper().logInfo(s"$name: $result")


// Databricks notebook source
// MAGIC %md
// MAGIC # Funciones para la plantilla Notebook para desarrollo a medida
// MAGIC
// MAGIC #### Objetivo
// MAGIC Este notebook aporta funciones genéricas para para implementar un desarrollo a medida
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC #### Optimizaciones de notebooks
// MAGIC https://spark.apache.org/docs/3.3.1/cloud-integration.html

// COMMAND ----------

spark.conf.set("spark.sql.parquet.inferTimestampNTZ.enabled", "false")

// COMMAND ----------

// MAGIC %md
// MAGIC ##Disponibilización del valor _mapper_ para el parseo de documentos JSON

// COMMAND ----------

import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.ArrayBuffer
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
// import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper - deprecated
import com.fasterxml.jackson.module.scala.{ClassTagExtensions, DefaultScalaModule}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, DataFrameWriter,Column}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col,explode,lit,coalesce,when,regexp_replace,sum,substring,concat,max,count,row_number,current_timestamp,dayofmonth,greatest,date_add, min, collect_list, desc, concat_ws, countDistinct, current_date, sequence, date_trunc,trim,lower,avg,from_utc_timestamp, hour, minute, second, lag,upper, instr, regexp_extract, split,from_json,asc}
import org.apache.spark.sql.functions.{last_day, localtimestamp, make_date, make_dt_interval, make_interval, make_timestamp, make_timestamp_ltz, make_timestamp_ntz, make_ym_interval, minute, months_between, next_day, now, quarter, second, session_window, timestamp_micros, timestamp_millis, timestamp_seconds, to_date, to_timestamp, to_timestamp_ltz, to_timestamp_ntz, to_unix_timestamp, to_utc_timestamp, trunc, try_to_timestamp, unix_date, unix_micros, unix_millis, unix_seconds, weekofyear}
import org.apache.spark.sql.expressions.Window
import scala.util.Try
import io.delta.tables._
import scala.util.matching.Regex
import com.repsol.datalake.log._
import com.repsol.datalake.control._
import com.repsol.datalake.mail._
import com.repsol.datalake.exceptions.HandleConcurrentExceptions
import com.fasterxml.jackson.core.`type`.TypeReference

// Scala 2.13
//import scala.jdk.CollectionConverters._ 

// Scala 2.12
import scala.collection.JavaConverters._  

// val mapper = new ObjectMapper with ScalaObjectMapper - deprecated
val mapper = new ObjectMapper() with ClassTagExtensions
mapper.registerModule(DefaultScalaModule)
mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

def getSpark: SparkSession = SparkSession.getActiveSession.getOrElse(SparkSession.builder().getOrCreate())

def castToListString(obj: Any): List[String] = 
  if (obj.isInstanceOf[java.util.List[String]])
    obj.asInstanceOf[java.util.List[String]].asScala.toList
  else
    obj.asInstanceOf[List[String]]

def castToMapString(obj: Any): Map[String, String] = 
  if (obj.isInstanceOf[java.util.Map[String, String]])
    obj.asInstanceOf[java.util.Map[String, String]].asScala.toMap
  else
    obj.asInstanceOf[Map[String,String]]

// COMMAND ----------

// MAGIC %md
// MAGIC # Función de lectura/Escritura del Dataset

// COMMAND ----------

// MAGIC %md
// MAGIC #### Funcion getAlias
// MAGIC Esta función devuelve el valor *source_alias* de documento JSON que define un origen de datos

// COMMAND ----------

def getAlias (conf: Map[String, Any], prefix: String = "source"): String = {
    // Retrieve the value associated with the key formed by prefix and "_alias" from the conf map
    // If the key does not exist, return a default error message
    conf.getOrElse(s"${prefix}_alias", s"ERROR ${prefix}_alias").toString
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función getLastPartitionPaths
// MAGIC Función que devuelve la lista con las últimas particiones del dataset
// MAGIC Parámetros:
// MAGIC - cs_tableName: Nombre de la tabla del dataset
// MAGIC - deepPartitions: profundidad de particionado (0 lo desactiva)
// MAGIC - numPartitions: Numero de particiones a recuperar

// COMMAND ----------

def getFilterConditionList(
  cs_table_name: String,
  deepPartitions: Int,
  numPartitions: Int,
  source_dl_name_tags: Seq[String] = Seq.empty
): Option[(Seq[String],Seq[String])] = {

  if (deepPartitions == 0 || numPartitions == 0) return None

  val partitionDFOpt = Try {
    val df = getSpark.sql(s"SHOW PARTITIONS $cs_table_name")
    if (df.columns.nonEmpty) df else null
  }.toOption

  val selectedCols: Array[String] = partitionDFOpt match {
    case Some(df) =>
      val partitionedCols = df.columns
      if (deepPartitions > partitionedCols.length) {
        throw new RuntimeException("No valid partitions found after filtering.")
      }
      partitionedCols.take(deepPartitions)

    case None =>
      val tableParts = cs_table_name.split("\\.")
      val (catalogName, schemaName, tableName) = tableParts.length match {
        case 3 => (tableParts(0).toLowerCase, tableParts(1).toLowerCase, tableParts(2).replaceAll("`", "").toLowerCase)
        case 2 => ("spark_catalog", tableParts(0), tableParts(1).replaceAll("`", "").toLowerCase)
        case 1 => ("spark_catalog", "default", tableParts(0).replaceAll("`", "").toLowerCase)
        case _ => throw new IllegalArgumentException("Invalid table name format")
      }

      val tagCols: Array[String] = if (source_dl_name_tags.nonEmpty) {
        source_dl_name_tags.toArray
      } else {
        Try {
          val columnQuery =
             s"""
              |SELECT column_name FROM $catalogName.information_schema.column_tags 
              |WHERE catalog_name = '$catalogName' 
              |AND schema_name = '$schemaName' 
              |AND table_name = '$tableName' 
              |AND tag_name = 't_etl_filter_hierarchy' 
              |ORDER BY tag_value ASC
             """.stripMargin

          val cols = getSpark.sql(columnQuery).collect().map(_.getString(0))
          if (cols.nonEmpty) cols else throw new RuntimeException("Empty info_schema fallback")
        }.getOrElse {
          throw new RuntimeException("No valid partition tags found after filtering")
        }
      }

      if (tagCols.isEmpty || deepPartitions > tagCols.length) {
        throw new RuntimeException("No valid partitions found after filtering. (fallback)")
      }
      tagCols.take(deepPartitions)
  }

  val filteredDF = {
    val baseDF = partitionDFOpt match {
      case Some(df) => df.selectExpr(selectedCols: _*)
      case None => getSpark.table(cs_table_name).selectExpr(selectedCols: _*)
    }
    // Cast each selected column to INT for sorting, to handle string-numeric ordering correctly
    val orderCols = selectedCols.map(colName => col(colName).cast("int").desc)
    baseDF.distinct().orderBy(orderCols: _*).limit(numPartitions)
  }

  val filterConditions = filteredDF.collect().map { row =>
    selectedCols.map(colName => s"($colName = '${row.getAs[Any](colName).toString}')").mkString(" and ")
  }

  if (filterConditions.nonEmpty) Some((filterConditions,selectedCols)) else None
}

def getFilterCondition(
  cs_table_name: String,
  deepPartitions: Int,
  numPartitions: Int,
  source_dl_name_tags: Seq[String] = Seq.empty
): Option[(String,Seq[String])] = {
  getFilterConditionList(cs_table_name, deepPartitions, numPartitions, source_dl_name_tags) match {
    case Some((filterList, cols)) =>
      val filterStr = filterList.map(cond => s"($cond)").mkString(" or ")
      if (filterStr.nonEmpty) Some((filterStr, cols))  else None
    case None => None
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función getLastPartitionsFilter
// MAGIC Función que devuelve una expresion _where_ para recuperar las últimas particiones del dataset
// MAGIC Parámetros:
// MAGIC - path: Ruta al dataset
// MAGIC - deepPartitions: profundidad de particionado (0 lo desactiva)
// MAGIC - numPartitions: Numero de particiones a recuperar

// COMMAND ----------

// The getLastPartitionsPathsFilter function original one to refer paritions based on a file structure path--
def getLastPartitionsPathsFilter(path: String, deepPartitions: Int, numPartitions: Int): Option[String] = {

  def getLastPartitionInternal(path: (String, String), deepInternal: Int): List[(String, String)] = {
    if (deepInternal == 0) {
      List(path)
    } else {
      val partition = dbutils.fs.ls(path._1)
      val filtered = partition.filter(x => x.isDir && x.name.contains("="))
      if (filtered.isEmpty) {
        throw new Exception("Invalid directory level")
      } else {
        val res = filtered.map(x => x.name.replace("/","").split("=")).sortWith(_(1).toInt > _(1).toInt)
        val internalPaths = res.map { x =>
          val value = if (Try(x(1).toDouble).isSuccess) x(1) else s""""${x(1)}""""
          (path._1 + "/" + x(0) + "=" + x(1), if (path._2.isEmpty) {
            s"(${x(0)} = ${value})"
          } else {
            s"${path._2} and (${x(0)} = ${value})"
          })
        }.toList
        internalPaths.flatMap(p => getLastPartitionInternal(p, deepInternal - 1))
      }
    }
  }

  if (deepPartitions == 0 || numPartitions == 0) {
    None
  } else {
    Some("(" + getLastPartitionInternal((path, ""), deepPartitions)
      .take(numPartitions)
      .map(_._2).mkString(") or (") + ")")
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC #Función getObject
// MAGIC Función:
// MAGIC
// MAGIC objectName: 
// MAGIC

// COMMAND ----------

def getObject(objectName: String): String = {
   val envValue = sys.env.getOrElse("DATABRICKS_ENV","")
   if (envValue.trim.isEmpty) {
    throw new Exception("No valid env variable found")
  }
   objectName.replace("%env%", envValue)
}


// COMMAND ----------

// MAGIC %md
// MAGIC #### Función getDataFromSource
// MAGIC
// MAGIC Función que crea una vista temporal basada en la configuración de origen da datos aportada.
// MAGIC
// MAGIC Parámetros admitidos:
// MAGIC - source_object
// MAGIC - source_format
// MAGIC - source_format_options
// MAGIC - source_deep_partition
// MAGIC - source_num_partitions
// MAGIC - source_alias

// COMMAND ----------

def getDataFromSource (map: Map[String, Any], filter: String = "", prefix: String = "source")(implicit digitalCase: String = "None") = {

    val sFormat:String = map.getOrElse(s"${prefix}_format", s"ERROR ${prefix}_format").toString
    val sFormatOptions:Map[String,String] = castToMapString(map.getOrElse(s"${prefix}_format_options", Map.empty[String, String]))
    val sDeepPartition:Int = map.getOrElse(s"${prefix}_deep_partition", "0").toString.toInt
    val numPartitions:Int = map.getOrElse(s"${prefix}_num_partitions", "1").toString.toInt
    val sAlias:String = map.getOrElse(s"${prefix}_alias", "table").toString
    val sObject:String = map.getOrElse(s"${prefix}_object", " ").toString
    val source_dl_name_tags: Seq[String] = castToListString(map.getOrElse(s"${prefix}_dl_name_tags", Seq.empty[String]))


    val sObjAux = getObject(sObject)
    val auxFormat = if (sFormat.equals("json_ext")) "json" else sFormat

    val where:Option[(String,Seq[String])] = if (auxFormat.equalsIgnoreCase("table")) {
      getFilterCondition(sObjAux, sDeepPartition, numPartitions,source_dl_name_tags)
      }
      else{
     Option(getLastPartitionsPathsFilter(sObjAux, sDeepPartition, numPartitions).getOrElse("1=1"),Seq("na"))
      }
      println("where "+where)

    val filterExt = if (filter.trim.isEmpty) {
        Some(where.getOrElse(("1=1",Seq()))._1)
    } else {
        if (where.isEmpty) {
            Some(filter)
        }else{
            Some(s"""( (${where.getOrElse(("1=1",Seq()))._1}) and ($filter) )""")
        }
    }
    println(s"Load $sObjAux with filter >$filterExt< as $sAlias")

    val dfAux = if (sFormat.equalsIgnoreCase("table")){
        getSpark.read.table(sObjAux)
    }
    else{
        getSpark.read.format(auxFormat).options(sFormatOptions).load(sObjAux)
    }
    val dfAuxFinal = if (sFormat.equals("json_ext")) {
        val cols = dfAux.select(explode(col("fields")).as("json")).select("json.*").collect.toList.map(_(0))
        var dfFinal = dfAux.select(explode(col("rows")).as("json")).select("json.*")
        cols.filterNot(x => dfFinal.columns.toList.contains(x)).foreach(x => dfFinal = dfFinal.withColumn(x.toString, lit(null).cast("string")))
        dfFinal
    }else{
       dfAux 
    }

    val df = if (filterExt.isEmpty) dfAuxFinal else dfAuxFinal.filter(filterExt.getOrElse("1=1"))
    df.createOrReplaceTempView(sAlias)
    df
} 

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función writeDataFrame
// MAGIC
// MAGIC Función que guarda el dataframe proporcionado en la ruta especificada en la configuración.
// MAGIC
// MAGIC Parámetros configurables en configuración:
// MAGIC - target_object
// MAGIC - target_format
// MAGIC - target_write_mode
// MAGIC - target_dl_name_tags
// MAGIC - target_format_options

// COMMAND ----------

def dynamicOverwriteData(df: DataFrame, targetObject: String, tDLNametags: List[String]): Unit = {
 // Check if tags are provided
  if (tDLNametags.isEmpty) {
    throw new IllegalArgumentException("column tags (tDLNametags) are required to perform dynamic overwrite operation.")
  }

  // Build a map colName -> DataType for quick lookup
  val schemaMap: Map[String, DataType] = df.schema.fields.map(f => f.name -> f.dataType).toMap

  // Select only the relevant partition columns, get distinct values
  val filters: Array[Row] = df.select(tDLNametags.map(col): _*).distinct().collect()

  // Check if conditions are available
  if (filters.nonEmpty) {
    // Build condition string, quoting string-valued literals
    val conditions: String = filters.map { row =>
      val parts = tDLNametags.zipWithIndex.map { case (colName, i) =>
        val dt    = schemaMap.getOrElse(colName, StringType)
        val lit   = dt match {
          case StringType | TimestampType | DateType => s"'${row.get(i).toString.replace("'", "\\'")}'"
          case _          => row.get(i).toString
        }
        s"$colName = $lit"
      }

    s"(${parts.mkString(" AND ")})"
  }.mkString(" OR ")

  val whereClause = s"DELETE FROM $targetObject WHERE $conditions"
  println(s"Executing delete: $whereClause")
  
  getSpark.sql(whereClause)
  } else {
    return
  }
}

// COMMAND ----------

def writeDataFrame(
    df: DataFrame,
    conf: Map[String, Any],
    optimize: Boolean = true,
    prefix: String = "target"
): Unit = {

  val tFormat: String = conf.getOrElse(s"${prefix}_format", "delta").toString.toLowerCase
  val tMode: String = conf.getOrElse(s"${prefix}_write_mode", "overwrite").toString.toLowerCase
  val tDLNametags: List[String] = castToListString(conf.getOrElse(s"${prefix}_dl_name_tags", List()))
  val tFormatOptions: Map[String, String] = castToMapString(conf.getOrElse(s"${prefix}_format_options", Map.empty[String, String]))
  val tObject: String = conf.getOrElse(s"${prefix}_object", "").toString.trim
  val tRetries: Integer = conf.getOrElse(s"${prefix}_concurrency_retries", 1).asInstanceOf[Int]

  val t_eObject = getObject(tObject)

  // table‐exists check only applies when writing to a UC table
  if (tFormat == "table" && !getSpark.catalog.tableExists(t_eObject)) {
    throw new IllegalArgumentException(
      s"Target table '$t_eObject' does not exist."
    )
  }

  // Helper that actually calls saveAsTable vs .format(...).save(...)
  def standardWrite(writer: DataFrameWriter[Row], retries: Integer = 1): Unit = {
    if (tFormat == "table") {
      HandleConcurrentExceptions.handleException {
        writer.saveAsTable(t_eObject)
      }(ignoreException = false, maxRetries = retries)
    } else {
      writer.format(tFormat).save(t_eObject)
    }
  }

  // Combined overwrite logic
  tMode match {

    case "overwrite" =>
      if (tDLNametags.nonEmpty && tFormat != "table") {
        // volume static or dynamic‐partition overwrite
        val writer = df.write
          .mode("overwrite")
          .option("mergeSchema", "true")
          .options(tFormatOptions)
          .partitionBy(tDLNametags: _*)

        standardWrite(writer, tRetries)

      } else {
        // classic overwrite (either no partitions, or writing to a UC table)
        val writer = df.write
          .mode("overwrite")
          .option("mergeSchema", "true")
          .options(tFormatOptions)

        standardWrite(writer, tRetries)
      }

    case "dynamic" =>
      // your existing dynamic‐delete/append logic
      dynamicOverwriteData(df, t_eObject, tDLNametags)
      val writer = df.write
        .mode("append")
        .option("mergeSchema", "true")
        .options(tFormatOptions)
      standardWrite(writer, tRetries)

    case "append" =>
      println(s"Appending data to $t_eObject")
      val writer = df.write
        .mode("append")
        .option("mergeSchema", "true")
        .options(tFormatOptions)
      standardWrite(writer, tRetries)

    case other =>
      throw new IllegalArgumentException(s"Unsupported write mode: $other")
  }

  println(s"Written to $t_eObject with mode: $tMode")
}


// COMMAND ----------

// MAGIC %md
// MAGIC #### Función readConfiguration
// MAGIC
// MAGIC Función que lee un fichero de configuración JSON y lo devuelve en forma de estructura de datos
// MAGIC

// COMMAND ----------

trait TypeReferenceProvider[T] {
  def typeRef: TypeReference[T]
}

object TypeReferenceProvider {
  implicit def materializeTypeReference[T]: TypeReferenceProvider[T] =
    new TypeReferenceProvider[T] {
      override val typeRef: TypeReference[T] = new TypeReference[T]() {}
    }
}

def readFile (path: String) = {
  // Construct the internal path to the file
  val vol_catalog= "datahub01%env%aucdatagold"
  val vCatalog=getObject(vol_catalog)
  val internalPath = s"/Volumes/$vCatalog/$path"
  println(s"File Path:$internalPath")


  // Read the file content into a string
  val source = Source.fromFile(internalPath)
  try {
    source.mkString
  } finally {
    source.close()
  }
}

def readConfiguration[T](path: String)(implicit ref: TypeReferenceProvider[T]): T = {
  val json = readFile(path)
  mapper.readValue(json, ref.typeRef)
}

/*
use example:
  import TypeReferenceProvider._
  readConfiguration[java.util.ArrayList[Map[String, String]]](s"config_pan0001/config_files/conf_test.json").asScala.toList
*/



// COMMAND ----------

// MAGIC %md
// MAGIC ### SCD1 multiple variants :
// MAGIC - SCD1 with Update/Insert Only Selected Columns
// MAGIC - SCD1 with Update Only if a Field Changed (whenMatched Condition)
// MAGIC - SCD1 with Custom Update with Condition AND Expressions Combined
// MAGIC - Save or merge a Delta table using SCD1 logic, optionally handling deletes.

// COMMAND ----------

//1. Update/Insert Only Selected Columns

def scd1SelectedColumnsMerge(
  sourceDF: DataFrame,
  targetTable: String,
  joinCols: Seq[String],
  updateCols: Seq[String],
  insertCols: Seq[String]
): Unit = {

  val deltaTable = io.delta.tables.DeltaTable.forName(getSpark, targetTable)
  val condition = joinCols.map(c => s"target.$c = source.$c").mkString(" AND ")

  deltaTable.as("target")
    .merge(sourceDF.as("source"), condition)
    .whenMatched()
    .updateExpr(updateCols.map(c => c -> s"source.$c").toMap)
    .whenNotMatched()
    .insertExpr(insertCols.map(c => c -> s"source.$c").toMap)
    .execute()
}


//2. Update Only if a Field Changed (whenMatched Condition)

def scd1ConditionalUpdateMerge(
  sourceDF: DataFrame,
  targetTable: String,
  joinCols: Seq[String],
  conditionExpr: String = "1=1"
): Unit = {

  val deltaTable = io.delta.tables.DeltaTable.forName(getSpark, targetTable)
  val condition = joinCols.map(c => s"target.$c = source.$c").mkString(" AND ")

  deltaTable.as("target")
    .merge(sourceDF.as("source"), condition)
    .whenMatched(conditionExpr)
    .updateAll()
    .whenNotMatched()
    .insertAll()
    .execute()
}


//3.Custom Update with Condition AND Expressions Combined
def scd1FullCustomMerge(
  sourceDF: DataFrame,
  targetTable: String,
  joinCols: Seq[String],
  matchCondition: String ="1=1",
  updateExprs: Map[String, String],
  insertExprs: Map[String, String]
): Unit = {

  val deltaTable = io.delta.tables.DeltaTable.forName(getSpark, targetTable)
  val condition = joinCols.map(c => s"target.$c = source.$c").mkString(" AND ")

  deltaTable.as("target")
    .merge(sourceDF.as("source"), condition)
    .whenMatched(matchCondition).updateExpr(updateExprs)
    .whenNotMatched().insertExpr(insertExprs)
    .execute()
}



// specific to digital case CLI0015 can be used for other cases wherever applicable

// 6. Save or merge a Delta table using SCD1 logic, optionally handling deletes.
  /**
   * Save or merge a Delta table using SCD1 logic, optionally handling deletes.
   *
   * @param rawTable Incoming DataFrame with changes (must include operation column).
   * @param tablePks Sequence of primary key columns.
   * @param targetTablePath Path to the Delta table (can be Unity Catalog or path-based).
   * @param overwrite If true, overwrite existing data.
   * @param dropDeletes If true, rows marked for deletion will be dropped before write/merge.
   * @param partitioning Optional partition column.
   * @param whenMatchedExpr Custom expression string for whenMatched delete condition.
   * @param whenNotMatchedExpr Custom expression string for whenNotMatched insert condition.
   */
  def customScd1WithDelete(
      rawTable: DataFrame,
      tablePks: Seq[String],
      targetTableName: String,
      overwrite: Boolean=false,
      dropDeletes: Boolean = true,
      partitioning: Option[String] = None,
      whenMatchedExpr: String = "source.DI_OPERATION_TYPE = 'D'",
      whenNotMatchedExpr: String = "source.DI_OPERATION_TYPE != 'D'",
      opField: String = "DI_OPERATION_TYPE"
  ): Unit = {

    val table = rawTable.withColumn(opField, coalesce(col(opField), lit("N")))
    val tableExists=getSpark.catalog.tableExists(targetTableName)
    // If overwrite or table doesn't exist, do a full write
    if (overwrite || !tableExists) {
          val tableToWrite = if (dropDeletes) table.filter(s"$opField != 'D'") else table

          val writer = tableToWrite.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")

          val partitionedWriter = partitioning match {
            case Some(partCol) => writer.partitionBy(partCol)
            case None          => writer
          }

          println("Overwriting or creating new table.")
          HandleConcurrentExceptions.handleException {
            partitionedWriter.saveAsTable(targetTableName)
          }(ignoreException = false)
          
        } else {
      // Merge into existing Delta table
      try {
        val deltaTable = DeltaTable.forName(getSpark, targetTableName)

        val matchConditionStr = tablePks
          .map(pk => s"source.$pk = target.$pk")
          .mkString(" AND ")

        val mergeBuilder = deltaTable
          .as("target")
          .merge(table.as("source"), matchConditionStr)

        val withDelete = if (dropDeletes) {
          mergeBuilder.whenMatched(whenMatchedExpr).delete()
        } else {
          mergeBuilder
        }

        withDelete
          .whenMatched()
          .updateAll()
          .whenNotMatched(whenNotMatchedExpr)
          .insertAll()
          .execute()

        
      } catch {
        case e: Exception =>
          println(s"Error during merge: ${e.getMessage}")
          throw e
      }
    }
  }



// COMMAND ----------

// MAGIC %md
// MAGIC ### Extract cluster key columns from DDL

// COMMAND ----------

def extractClusterByColumns(tableName: String): Seq[String] = {
  try {
    val ddl = getSpark.sql(s"SHOW CREATE TABLE $tableName").collect().map(_.getString(0)).mkString("\n")

    // Regex to find CLUSTER BY (col1, col2, ...)
    val clusterByRegex = "(?i)CLUSTER BY\\s*\\(([^)]+)\\)".r

    clusterByRegex.findFirstMatchIn(ddl) match {
      case Some(m) =>
        val colsStr = m.group(1)
        println(s"cluster columns are : $colsStr")
        colsStr.split(",").map(_.trim)
      case None =>
        println(s"No CLUSTER BY clause found in DDL for $tableName")
        Seq.empty
    }
  } catch {
    case e: Exception =>
      println(s"Error extracting cluster by columns: ${e.getMessage}")
      Seq.empty
  }
}


// COMMAND ----------

// MAGIC %md
// MAGIC ### Liquid Clustering method for running optimize / Analyze / Vacuum

// COMMAND ----------

def getFirst32NonBooleanColumns(df: DataFrame,taggedCols:Seq[String]): Seq[String] = {
  val colsToUse=(df.columns.toSeq).distinct.map { colum =>
  if (colum.contains("/") || colum.contains("|")) s"`$colum`" else colum
}
  val outcolumns=df.selectExpr(colsToUse:_*).schema.fields
    .filterNot(_.dataType == BooleanType)     // Exclude BooleanType
    .map(_.name)
    .take(32)    ++ df.selectExpr(taggedCols.map { colum =>
  if (colum.contains("/") || colum.contains("|")) s"`$colum`" else colum
}:_*).schema.fields
    .filterNot(_.dataType == BooleanType)     // Exclude BooleanType
    .map(_.name)                              // Get column names
    outcolumns
}

// Extract clustering columns from tag
def getTaggedColumns(
  tableName: String,
  tagKey: String
): Seq[String] = {

  try {
    val parts = tableName.split("\\.")
    require(parts.length == 3, s"Expected format catalog.schema.table, got: $tableName")

    val catalogName = parts(0)
    val schemaName = parts(1)
    val table = parts(2)

    val columnQuery =
      s"""
         |SELECT column_name
         |FROM $catalogName.information_schema.column_tags
         |WHERE catalog_name = '$catalogName'
         |  AND schema_name = '$schemaName'
         |  AND table_name = '$table'
         |  AND tag_name = '$tagKey'
         |ORDER BY tag_value ASC
         |""".stripMargin

    val cols = getSpark.sql(columnQuery).collect().map(_.getString(0)).toSeq

    if (cols.nonEmpty) cols else Seq.empty[String]
  } catch {
    case e: Exception =>
      println(s"Failed to retrieve tagged columns: ${e.getMessage}")
      Seq.empty[String]
  }
}



// Main maintenance function
def runLiquidMaintenance(
  tableName: String,
  tagName: String="analyzeCol"
): Unit = {

  try {
    if (!tableName.startsWith("/Volumes/"))
    {
    if (!getSpark.catalog.tableExists(tableName)) {
      println(s"Table $tableName does not exist.")
      return
    }
    }
    else{
      println(s"$tableName is a Volume Path!! Please Check")
      return
    }
    

    val tableDetails = getSpark.sql(s"DESCRIBE TABLE EXTENDED $tableName")
    val isDeltaTable = tableDetails
      .filter("col_name = 'Provider'")
      .select("data_type")
      .collect()
      .headOption
      .exists(row => row.getString(0).toLowerCase.contains("delta"))

    println(s"Starting maintenance for table: $tableName")

    if (isDeltaTable) {
      println("OPTIMIZE without ZORDER (liquid clustering)")
      getSpark.sql(s"OPTIMIZE $tableName")
    }

    println("Determining columns for ANALYZE...")
    println("Determining columns from Column tag...")
    val taggedCols = getTaggedColumns(
  tableName=tableName,
  tagKey=tagName
)
    println(s"tag columns $taggedCols")
    println("Determining first 32 non booleanType columns by combining tagged Columns")
    val firstthirtytwowithtagColumns=getFirst32NonBooleanColumns(getSpark.sql(s"select * from $tableName"),taggedCols.toSeq)
    println(s"Final Cols from Tag to be considered for Analyze $firstthirtytwowithtagColumns")
    val clusterByCols=extractClusterByColumns(tableName)
    println(s"cluster By columns : $clusterByCols")
    val clusterCols = (clusterByCols ++ firstthirtytwowithtagColumns).distinct
      

    if (clusterCols.nonEmpty) {
      val colsAnalyze = clusterCols.distinct.map { colum =>
  if (colum.contains("/") || colum.contains("|")) s"`$colum`" else colum
}.mkString(",")
      println(s"Running ANALYZE TABLE for columns: $colsAnalyze")
      getSpark.sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS $colsAnalyze")
    } else {
      println("No cluster columns found. Running ANALYZE on all columns.")
      getSpark.sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR ALL COLUMNS")
    }

    println(s"Maintenance completed for $tableName")

  } catch {
    case e: Exception =>
      println(s"Maintenance failed for $tableName: ${e.getMessage}")
      throw e
  }
}


// COMMAND ----------

// MAGIC %md
// MAGIC ### util for vacuum and retention

// COMMAND ----------

def runVacuumWithRetention(
  tableOrSchemaName: String,
  retentionDays: Int = 7,
  updateRetentionProperty: Boolean = false,
  runForAllTablesInSchema: Boolean = false
): Unit = {

  try {
    if (runForAllTablesInSchema) {
      println(s"Running VACUUM for all tables in one or more schema: $tableOrSchemaName")
tableOrSchemaName.split(",").foreach{ singleSchemaOrTable => 

val tables = getSpark.catalog.listTables(singleSchemaOrTable).collect()

      tables.foreach { table =>
        val fullTableName = s"${singleSchemaOrTable}.${table.name}"
        runVacuumOnTable(fullTableName, retentionDays, updateRetentionProperty)
      }

}
      // List all tables in the schema
      

    } else {
      // Run vacuum on a single or multiple tables in different catalog/schema
      tableOrSchemaName.split(",").foreach{ fullTableNamewithCatalog =>
runVacuumOnTable(fullTableNamewithCatalog, retentionDays, updateRetentionProperty)
      }
      
    }

  } catch {
    case e: Exception =>
      println(s"Vacuum operation failed: ${e.getMessage}")
      throw e
  }
}

def runVacuumOnTable(
  tableName: String,
   retentionDays: Int,
  updateRetentionProperty: Boolean
): Unit = {

  try {
    if (!getSpark.catalog.tableExists(tableName)) {
      println(s"Table $tableName does not exist.")
      return
    }

    if (updateRetentionProperty) {
      val retentionInterval = s"interval ${retentionDays} days"
      println(s"Setting 'delta.deletedFileRetentionDuration' = '$retentionInterval' for $tableName")
      getSpark.sql(
        s"""
           |ALTER TABLE $tableName
           |SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '$retentionInterval')
           |""".stripMargin)
    }
    val retentionDaysUpd = if (!updateRetentionProperty) 7 else retentionDays

    println(s"Running VACUUM on $tableName with retention = $retentionDaysUpd days")
    getSpark.sql(s"VACUUM $tableName RETAIN ${retentionDaysUpd * 24} HOURS")
    println(s"VACUUM completed on $tableName")

  } catch {
    case e: Exception =>
      println(s"Vacuum operation failed for $tableName: ${e.getMessage}")
      throw e
  }
}



// COMMAND ----------

// MAGIC %md
// MAGIC ### Method for Updating existing cluster keys and running OPTIMIZE/OPTIMIZE FULL based on the flag

// COMMAND ----------

def updateClusteringAndOptimize(
  tableName: String,
  newClusterKeys: Seq[String],
  runFullOptimize: Boolean
): Unit = {

  try {

    if (!getSpark.catalog.tableExists(tableName)) {
      println(s" Table $tableName does not exist.")
      return
    }
    val tableCols=getSpark.table(tableName).columns
    val OldclusterCols = extractClusterByColumns(tableName)
    val diffOfKeys=newClusterKeys.diff(OldclusterCols) ++ OldclusterCols.diff(newClusterKeys)
    if (diffOfKeys.isEmpty)
    {
      println(s" Cluster Keys are same as before !!")
      return
    }

    if (newClusterKeys.nonEmpty) {
      val clusterColsStr = (newClusterKeys).mkString(",") 
      println("newClusterKeys : "+clusterColsStr)
      getSpark.sql(s"ALTER TABLE $tableName SET TBLPROPERTIES('delta.dataSkippingStatsColumns' = '$clusterColsStr')")
      println(s" Updating clustering keys for $tableName to: ($clusterColsStr)")

      // Update clustering keys
      getSpark.sql(s"ALTER TABLE $tableName CLUSTER BY ($clusterColsStr)")
    } else {
      println(s" No clustering keys provided. Skipping ALTER TABLE.")
    }

    // Run OPTIMIZE with or without ZORDER
    if (runFullOptimize && newClusterKeys.nonEmpty) {
      println(s" Running OPTIMIZE FULL ")
      getSpark.sql(s"OPTIMIZE $tableName FULL ")
    } else if (newClusterKeys.nonEmpty) {
      println(s" Running standard OPTIMIZE (incremental)")
      getSpark.sql(s"OPTIMIZE $tableName")
    } else {
      println(s" OPTIMIZE skipped — no clustering keys set.")
    }

    // Optional ANALYZE
    println(s" Running ANALYZE TABLE to update column statistics")
     val tableColumns = getSpark.table(tableName).schema.fieldNames.take(32)

    // Merge with additional analyzeCols and remove duplicates
    val finalCols = (newClusterKeys).distinct

    val colsAnalyze = finalCols.mkString(",")
    getSpark.sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS $colsAnalyze")

    println(s" Clustering and optimization completed for $tableName")

  } catch {
    case ex: Exception =>
      println(s" Failed to update clustering or optimize table $tableName: ${ex.getMessage}")
      throw ex
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC ###This is a generic wrapper function that simplifies writing and merging data into Delta tables by routing to specific SCD1 implementations or write methods based on the functionName parameter.
// MAGIC
// MAGIC **Key Responsibilities:**
// MAGIC
// MAGIC - Dynamically dispatches to one of several specialized Delta write/merge strategies:
// MAGIC - Standard SCD1 conditional update
// MAGIC - SCD1 with selected insert/update columns
// MAGIC - SCD1 with custom update/insert expressions and conditions
// MAGIC - SCD1 with delete handling logic
// MAGIC - Direct write using configuration settings (e.g., mode, partitioning)
// MAGIC - Supports optional partitioning, delete filtering, overwrite mode, custom match conditions, and audit fields.
// MAGIC
// MAGIC **Parameters Overview:**
// MAGIC - sourceDF: The input DataFrame to write or merge.
// MAGIC - targetTable: Name of the target Delta table.
// MAGIC - joinCols: List of primary key or join columns.
// MAGIC - updateCols, insertCols: Specific columns to update or insert.
// MAGIC - updateExprs, insertExprs: Column-level custom expressions for update/insert.
// MAGIC - matchConditionExpr: Condition for matching rows in merge.
// MAGIC - whenMatchedExpr, whenNotMatchedExpr: Expressions to handle delete/insert cases.
// MAGIC - conf: Additional configurations as key-value pairs.
// MAGIC - optimize: Whether to run Delta table optimization after write.
// MAGIC - operationName: The strategy to apply (e.g., "scd1", "scd1_with_delete").
// MAGIC - analyzeCols: for running analyze on subset of columns either defined on config or runs for clusterBy cols or for all columns in case of non clustered tables
// MAGIC

// COMMAND ----------


def writeWrapper(
  sourceDF: DataFrame,
  targetTable: String= "",
  joinCols: Seq[String] = Seq() ,
  updateCols: Seq[String] = Seq(),
  insertCols: Seq[String] = Seq(),
  updateExprs: Map[String, String] = Map(),
  insertExprs: Map[String, String] = Map(),
  matchConditionExpr: String = "1=1",
  overwrite: Boolean = false,
  dropDeletes: Boolean = false,
  whenMatchedExpr: String = "source.DI_OPERATION_TYPE = 'D'",
  whenNotMatchedExpr: String = "source.DI_OPERATION_TYPE != 'D'",
  opField: String = "DI_OPERATION_TYPE",
  conf: Map[String, Any]=  Map(),
  optimize: Boolean = true,
  prefix: String = "",
  tagKey: String = "analyzeCol",
  operationName: String =""
): Unit = {

  var fTargetTable= if(targetTable.isEmpty) {val tObject: String = conf.getOrElse(s"${prefix}_object", "").toString.trim
  getObject(tObject)} else targetTable


  operationName match {
    case "scd1" =>
      scd1ConditionalUpdateMerge(sourceDF, fTargetTable, joinCols, matchConditionExpr)

    case "scd1_for_selected_columns" =>
      scd1SelectedColumnsMerge(sourceDF, fTargetTable, joinCols, updateCols, insertCols)

    case "scd1_with_insert_update_column_map" =>
      scd1FullCustomMerge(sourceDF, fTargetTable, joinCols, matchConditionExpr, updateExprs, insertExprs)

    case "scd1_with_delete" =>
      customScd1WithDelete(
        rawTable = sourceDF,
        tablePks = joinCols,
        targetTableName = fTargetTable,
        overwrite = overwrite,
        dropDeletes = dropDeletes,
        whenMatchedExpr = whenMatchedExpr,
        whenNotMatchedExpr = whenNotMatchedExpr,
        opField = opField
      )

    case "direct_write_using_writeModes" =>
      writeDataFrame(sourceDF, conf, optimize, prefix)

    case _ =>
      throw new IllegalArgumentException(s"Unknown Operaion: $operationName")
  }
  if(optimize)
  {
    runLiquidMaintenance(
  tableName=fTargetTable,
  tagName=tagKey
)
}
else{
  println("optimize is disabled")
}
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Auxiliary functions to merge datasets

// COMMAND ----------

def getDefaultDatasetName (prefix:String = "target") = s"__${prefix}_noname__"

def parseMapString(str: String): Map[String, String] =
  if (str.trim.isEmpty) Map.empty else mapper.readValue(str, new TypeReference[java.util.Map[String, String]]() {}).asScala.toMap

def parseMapAny(str: String): Map[String, Any] =
  if (str.trim.isEmpty) Map.empty else mapper.readValue(str, new TypeReference[java.util.Map[String, Object]]() {}).asScala.toMap

def parseListMap(str: String): List[Map[String, Any]] = 
  if (str.trim.isEmpty) List.empty else mapper.readValue(str, new TypeReference[java.util.List[java.util.Map[String, Object]]]() {}).asScala.toList.map(_.asScala.toMap)


def joinDatasets (dataset: String = "", datasets: String = "", prefix: String = "target"): Map[String, Map[String, Any]] = {
  def mergeDatasets(single: Map[String, Any], multi: List[Map[String, Any]]): Seq[Map[String, Any]] =
    (single.isEmpty, multi.isEmpty) match {
      case (true, true)   => Seq.empty
      case (false, true)  => Seq(single)
      case (true, false)  => multi
      case (false, false) => single :: multi
    }

  def toAliasMap(seq: Seq[Map[String, Any]], prefix:String = "target"): Map[String, Map[String, Any]] =
    seq.zipWithIndex.map { case (x, idx) =>
      x.getOrElse(s"${prefix}_alias", f"$idx%02d${getDefaultDatasetName()}").toString -> x
    }.toMap
  
  val dAux = parseMapAny(dataset)
  val dsAux = parseListMap(datasets)

  val datasetsAux = mergeDatasets(dAux, dsAux)

  toAliasMap(datasetsAux, prefix)
}

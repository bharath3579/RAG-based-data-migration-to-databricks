// Databricks notebook source
// MAGIC %md
// MAGIC # Funciones para el proyecto EYP0007 para desarrollo a medida
// MAGIC
// MAGIC #### Objetivo
// MAGIC Este notebook contiene funciones de uso comun y reutilizables dentro del proyecto EYP0007

// COMMAND ----------

// MAGIC %md
// MAGIC # Funciónes de lectura/Escritura del Dataset

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función writeDataFramePartitionDummy
// MAGIC
// MAGIC Función que añade un particion dummy al dataframe proporcionado en la ruta especificada en la configuración.

// COMMAND ----------

// MAGIC %md
// MAGIC # Funciónes de tranformaciones del Dataset

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función transformationRenameColumns
// MAGIC
// MAGIC Función que permite renombrar columnas tomando los valores de un mapa.

// COMMAND ----------

def applyDDLFromTableToDF(
  df: DataFrame,
  tableName: String
): DataFrame = {

  // Get target schema
  val targetDF = getDataFromSource(
    Map("source_format" -> "Table", "source_object" -> tableName)
  )
  val targetSchema = targetDF.schema

  // Convert target schema to map for fast lookup
  val targetSchemaMap = targetSchema.fields.map(f => f.name -> f.dataType).toMap

  // Build final column expressions
  val finalCols = df.columns.map { colName =>
    targetSchemaMap.get(colName) match {
      case Some(targetType) =>
        col(colName).cast(targetType).alias(colName)  // cast if exists in both
      case None =>
        col(colName) // keep as-is if only in source
    }
  }

  df.select(finalCols: _*)
}

// COMMAND ----------

def addMissingColumns(df: DataFrame,colAndDatatype: Map[String, String]): DataFrame = {
  colAndDatatype.foldLeft(df) { case (tempDf, (column, datatype)) =>
    if (tempDf.columns.contains(column)) tempDf
    else tempDf.withColumn(column, lit(null).cast(datatype.trim.toLowerCase))
  }
}

// COMMAND ----------

def transformationRenameColumns (df: DataFrame, mapFields: Map[String, String]): DataFrame = {
    mapFields.foldLeft(df) { case (tempDf, (oldName, newName)) =>
        tempDf.withColumnRenamed(oldName, newName)
    }
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función joinDataframes
// MAGIC
// MAGIC Función que permite unir dos dataframes.

// COMMAND ----------

def joinDataframes(df1: DataFrame, df2: DataFrame, joinConditions: Map[String, String], selectFields: Seq[String], joinType: String): DataFrame = {
    val joinedDF = df1.join(df2, joinConditions.map { case (col1, col2) => df1(col1) === df2(col2) }.reduce(_ && _), joinType)
    val selectedDF = joinedDF.select((df1.columns.map(df1(_)) ++ selectFields.map(df2(_))):_*)
    selectedDF

}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función toTransposeDataframe
// MAGIC
// MAGIC Función que permite transponer un dataframe de columnas a filas

// COMMAND ----------

import org.apache.spark.sql.functions.{col, lit, expr, when, concat, stack, explode,first,date_format}
def toTransposeDataframe(df: DataFrame, columnsFixed: Seq[String], columnsTransposed: Seq[String]): DataFrame = {
  val transposeDf = df.select(
    (columnsFixed.map(col) ++
    Seq(expr(s"stack(${columnsTransposed.length}," + 
      columnsTransposed.map(colName => s"'$colName', $colName").mkString(", ") + ") as (attribute, value)"))): _*
  ).select((columnsFixed.map(col) ++ Seq(upper(col("attribute")), col("value"))): _*).withColumnRenamed("upper(attribute)", "attribute")
  transposeDf
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función selectFieldsDataframe
// MAGIC
// MAGIC Función que permite seleccionar una lista de campos de un dataframe

// COMMAND ----------

def selectFieldsDataframe(df: DataFrame, fields: Seq[String]): DataFrame = {
    df.select(fields.map(col): _*)
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función castDataFrame
// MAGIC
// MAGIC Función que permite castear los campos de un dataframe

// COMMAND ----------

def castDataFrame(df: DataFrame, castMap: Map[String, String]): DataFrame = {
  castMap.foldLeft(df) { case (tempDf, (column, dataType)) =>
    tempDf.withColumn(column, col(column).cast(dataType))
  }
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función toPivotDataframe
// MAGIC
// MAGIC Función que permite pivotar un dataframe de filas a columnas.

// COMMAND ----------

def toPivotDataframe(df: DataFrame, columnsFixed: Seq[String]): DataFrame = {
  val pivotedDF = df.groupBy(columnsFixed.map(col): _*)
  .pivot(when(col("unit").isNotNull, lower(concat($"attribute", lit("_"), $"unit"))).otherwise(lower($"attribute")))
  .agg(first("value_final"))
  pivotedDF
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Funciónes de adiciones al Dataset

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función addFieldIdVers
// MAGIC
// MAGIC Función que permite añadir un campo llamado id_vers, tomando como base los pasados por parametro

// COMMAND ----------

def addFieldIdVers (df: DataFrame, idVersFields: Map[String, String]): DataFrame = {
    val (keyIdVers1, keyIdVers2) = idVersFields.head

    df.withColumn("id_vers_date_aux", date_format(col(keyIdVers1), "yyyyMMdd"))
        .withColumn("id_vers_id_aux", col(keyIdVers2).cast("string"))
        .withColumn("id_vers", concat(col("id_vers_date_aux"), col("id_vers_id_aux")))
        .drop("id_vers_date_aux", "id_vers_id_aux")
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función addRowDummy
// MAGIC
// MAGIC Función que permite añadir una row dummy con valores por defecto.

// COMMAND ----------

def addRowDummy(df: DataFrame): DataFrame = {
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.types.{StringType, TimestampType}


  val newRow = Row.fromSeq(df.schema.map { field =>
    (field.name, field.dataType) match {
      case (name, StringType) if name.startsWith("id_") => "-99"
      case (name, StringType) if name.startsWith("des_") => "Not Available"
      case (name, TimestampType) if name.startsWith("fec_") => java.sql.Timestamp.valueOf("1900-01-01 00:00:00")
      case (_, StringType) => "" 
      case (_, TimestampType) => java.sql.Timestamp.valueOf("1970-01-01 00:00:00") 
      case _ => null 
    }
  })


  val dfDummy = spark.createDataFrame(Seq(newRow).asJava,df.schema)

  df.union(dfDummy)
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Funciónes de obtencion

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función getSubstringByRegex()
// MAGIC
// MAGIC Función que permite obtener una subcadena que coincida con la expresion regular proporcionada.

// COMMAND ----------

def getSubstringByRegex(sourceString: String, regex: scala.util.matching.Regex): Option[String] = {
    regex.findFirstMatchIn(sourceString).map(_.group(1))
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función getWhereFilterDays()
// MAGIC
// MAGIC Función que obtiene una condicion de filtro de fecha que dada una fecha de referencia le resta los dias indicados y genera la conficion >= para cada uno de los campos.

// COMMAND ----------

def getWhereFilterDays (date: String, fields: List[String], days: Integer, inputDateFormat: String = "yyyy/MM/dd"): String = {
    

  val inputFormatter = java.time.format.DateTimeFormatter.ofPattern(inputDateFormat)
  val dateObj = java.time.LocalDate.parse(date, inputFormatter)
  

  val targetDate = dateObj.minusDays(days.toLong)


  val outputFormatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val formattedDate = targetDate.format(outputFormatter)
  
  val conditions = fields.map(field => s"$field >= '$formattedDate'").mkString(" or ")
  
  conditions

}

// COMMAND ----------

// MAGIC %md
// MAGIC # Funciónes con fechas

// COMMAND ----------

// MAGIC %md
// MAGIC #### Función returnDateNowString()
// MAGIC
// MAGIC Función que permite obtener la fecha actual en formato String.

// COMMAND ----------

def returnDateNowString(outputDateFormat: String = "yyyy/MM/dd"): String = {
    val currentDate = java.time.LocalDate.now()
    val formatter = java.time.format.DateTimeFormatter.ofPattern(outputDateFormat)
    currentDate.format(formatter)
}

// COMMAND ----------

// MAGIC %md
// MAGIC # Funciones Delta

// COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", true)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import io.delta.tables._
import java.text.SimpleDateFormat
import java.util.Calendar

// COMMAND ----------


import io.delta.tables._
import org.apache.spark.sql.Column
import java.util.Calendar
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
 
def toScalaMap(obj: Any): Map[String, String] = obj match {
    case m: java.util.Map[_, _] => m.asScala.toMap.map { case (k, v) => k.toString -> v.toString }
    case m: Map[_, _] => m.map { case (k, v) => k.toString -> v.toString }
}
    
def toScalaList(obj: Any): List[String] = obj match {
    case l: java.util.List[_] => l.asScala.toList.map(_.toString)
    case l: Seq[_] => l.map(_.toString).toList
}
def getDeltaTable(df: DataFrame, config: Map[String, Any]): (DataFrame, DeltaTable) = {

    val tFormat:String = "table"
    val target_object = config.getOrElse("target_object","ERROR target_object").toString
    val target_operation_name = config.getOrElse("target_operation_name", "ERROR target_operation_name").toString
    
    var targetDf: DataFrame = null 
    var targetDelta: DeltaTable = null 
    val sourceMap = Map(
                        "source_object" -> config("target_object").toString,
                        "source_format" -> "table"
                    )
    if (spark.catalog.tableExists(getObject(target_object))) {
        targetDf = getDataFromSource(sourceMap)
        println("retriving delta table")
        targetDelta = DeltaTable.forName(getObject(target_object))
    } else {
        writeWrapper(sourceDF=df, conf=config, prefix="target", operationName = target_operation_name, optimize=true)
        targetDf = getDataFromSource(sourceMap)
        println("retriving delta table")
        targetDelta = DeltaTable.forName(getObject(target_object))
    } 

    (targetDf, targetDelta)
}


 
def applyChange (table: Map[String, Any], sDf: DataFrame): Unit = {

    // table.as("target")
    // .merge(sourceDf.as("source"), s"source.__calculated_key__ = target.__calculated_key__")
    // // .merge(sourceDf.as("source"), s"source.id = target.id")
    // .whenMatched()
    // .updateAll
    // .whenNotMatched()
    // .insertAll
    // .execute()

    writeWrapper(sourceDF=sDf,conf=table,prefix="target", joinCols = Array("__calculated_key__"), operationName="scd1")
}
 
def optimizeDeltaFun (table: DeltaTable , table_name:String,  dayOfWeek: String): Unit = {
     val now = Calendar.getInstance.getTime
     val dowInt = new SimpleDateFormat("u").format(now)
 
       if (dayOfWeek == dowInt) {
         try {
            table.optimize.executeCompaction()
        } catch {
         case e: Throwable =>e.printStackTrace()
            }
        runVacuumOnTable(table_name, 7, false)
    } 
}
 
//  NOT USED ANY WHERE ######

def getDataFilter (df: DataFrame, fullLoad: Boolean): DataFrame = {
    val now = Calendar.getInstance.getTime
    val dateString = new SimpleDateFormat("yyyy-MM-dd").format(now)
    val date = LocalDate.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val dowInt = new SimpleDateFormat("u").format(now)
    val dateFilter = date.minusDays(3)
 
    var dfAux: DataFrame = null
    if (fullLoad) {
        dfAux = df
    }else {
        dfAux = df.filter((col("fec_create_date") >= dateFilter) || (col("fec_update_date") >= dateFilter))
    }
 
    dfAux
}

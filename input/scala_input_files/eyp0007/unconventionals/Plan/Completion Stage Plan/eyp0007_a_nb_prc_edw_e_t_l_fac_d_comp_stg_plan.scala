// Databricks notebook source
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

LogHelper().logStart()

println(s"applicationName = $applicationName")
println(s"uuid = $uuid")
println(s"puid = $puid")
println(s"step = $step")
println(s"uniqueKey = $uniqueKey")
println(s"sources = $sources")
println(s"target = $target")
println(s"name = $name")

// COMMAND ----------

// MAGIC %md
// MAGIC # Código que implementa la lógica necesaria

// COMMAND ----------

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
val order = Seq("cod_vers_wellbore", "cod_vers_well_string", "cod_vers_well", "cod_vers", "cod_uwbi", "cod_uwbi_num_stage", "num_stage","id_asset","val_interval_top","val_interval_base","fec_time_start","val_stage_slickwater_volume",
"val_stage_100mesh_sand_volume","val_stage_40_70_sand_volume","val_stage_30_50_sand_volume","val_stage_20_40_sand_volume","val_fresh_water","val_15hcl_acid"
)

// Renombramientos para dfFinal
def transformationdf(df: DataFrame): DataFrame = {
  var dfTrn: DataFrame = df

    dfTrn = dfTrn.withColumnRenamed("data.stage_number_stages", "num_stage")
               .withColumnRenamed("asset_id_stages", "id_asset")
               .withColumnRenamed("data.top_perforation_stages", "val_interval_top")
               .withColumnRenamed("data.bottom_perforation_stages", "val_interval_base")
               .withColumnRenamed("timestamp_stages", "fec_time_start")
               
  return dfTrn
}

def Createcodvers(df: DataFrame): DataFrame = {
  val dfConCodVers = df.withColumn(
    "cod_vers",
    concat(col("cod_vers_wellbore"), col("`data.stage_number_stages`").cast("string"), col("asset_id_stages").cast("string"))
  )
  dfConCodVers
}

def buildDummyRow(schema: StructType, fixedCount: Int = 4): Row = {
  val values = schema.fields.zipWithIndex.map { case (_, idx) =>
    if (idx < fixedCount) "-99" else null
  }
  Row.fromSeq(values)
}

def addRowDummy(df: DataFrame): DataFrame = {
  val dummyRow = buildDummyRow(df.schema)
  val dfDummy = spark.createDataFrame(
    spark.sparkContext.parallelize(Seq(dummyRow)),
    df.schema
  )
  df.union(dfDummy)
}


// COMMAND ----------


//Limpiamos api_number de assets
def transformAssed (df: DataFrame): DataFrame = {

    df.withColumn("api_number", trim(regexp_replace(col("api_number"), "\\s+", ""))).withColumn("api_number", regexp_replace(col("api_number"), "[^0-9]", ""))
    
    }

//cod_api12 a cod_api10 en wellbore
def transformwellb (df: DataFrame): DataFrame = {

    df.withColumn("cod_api12", trim(regexp_replace(col("cod_api12"), "\\s+", ""))).withColumn("cod_api10", substring(col("cod_api12"), 1, 10))
    
    }

//Join de api number assets - welbore
def joinAssedWellb(df1: DataFrame, df2: DataFrame): DataFrame = {
        val dfMatches = df2.join(df1, df2("cod_api10") === df1("api_number"))
        val dfSelected = dfMatches.select("id_vers_rel", "asset_id", "api_number", "fec_start_date_rel", "cod_uwbi", "id_vers_well", "id_vers_well_hole")
        val dfAssetWelbore = dfSelected.withColumnRenamed("asset_id","asset_id_wellbore")
        dfAssetWelbore
        }

// COMMAND ----------

def unionStageWellbAssed(df1: DataFrame, df2: DataFrame): DataFrame = {
  //Unión de stages y predictions
  val dfstage = df2
  val dfRenamedstages = dfstage.columns.foldLeft(dfstage) { (tempDf, colName) =>
    tempDf.withColumnRenamed(colName, s"${colName}_stages")
  }

  //Conversión data.start_time_stages para solo coger el mayor o igual al fec_start_date_rel de wellbore
  val dfstages = dfRenamedstages.withColumn("timestamp_stages",to_timestamp(from_unixtime(col("timestamp_stages"))))

  // Crear la ventana particionada por api_number y ordenada por fec_start_date_rel en orden descendente
  val windowdateasset = Window.partitionBy("asset_id_stages", "`data.stage_number_stages`").orderBy(desc("fec_start_date_rel"))

  val dfFiltered = dfstages.join(df1, dfstages("asset_id_stages") === df1("asset_id_wellbore"),"left")
  .withColumn("cod_vers_wellbore", coalesce(df1("id_vers_rel"), lit("-99"))).withColumn("cod_vers_well_string", coalesce(df1("id_vers_well"), lit("-99"))).withColumn("cod_vers_well", coalesce(df1("id_vers_well_hole"), lit("-99")))

  // Filtrar solo la fila con row_number = 1
  val dfFinal = dfFiltered.withColumn("row_num", row_number().over(windowdateasset)).filter(col("row_num") === 1)
  dfFinal
  }



// COMMAND ----------

def createUnionColumnpropants(df1: DataFrame, df2: DataFrame): DataFrame = {


    val dfPropRe = df2.select(
        col("asset_id").as("asset_id_proppants"),
        col("`data.stage_number`").as("data.stage_number_proppants"),
        col("amount").as("amount_proppants"),
        col("type").as("type_proppants")
        )
 
    // Hacer el join
    val dfFunion = df1
    .join(dfPropRe, 
            df1("asset_id_stages") === dfPropRe("asset_id_proppants") && 
            df1("`data.stage_number_stages`") === dfPropRe("`data.stage_number_proppants`"), 
            "left") // Mantener todas las filas de df1

    // Rellenar las columnas según el tipo de proppant
    val resultDF = dfFunion
    .withColumn("val_stage_100mesh_sand_volume",
                when(col("type_proppants").contains("100"), col("amount_proppants")).otherwise(null))
    .withColumn("val_stage_40_70_sand_volume",
                when(col("type_proppants").contains("40/70"), col("amount_proppants")).otherwise(null))
    .withColumn("val_stage_30_50_sand_volume",
                when(col("type_proppants").contains("30/50"), col("amount_proppants")).otherwise(null))
    .withColumn("val_stage_20_40_sand_volume",
                when(col("type_proppants").contains("20/40"), col("amount_proppants")).otherwise(null))
    .groupBy(df1("`data.stage_number_stages`"), df1("asset_id_stages"))
    .agg(
        coalesce(first("val_stage_100mesh_sand_volume", ignoreNulls = true), lit(0)).alias("val_stage_100mesh_sand_volume"),
        coalesce(first("val_stage_40_70_sand_volume", ignoreNulls = true), lit(0)).alias("val_stage_40_70_sand_volume"),
        coalesce(first("val_stage_30_50_sand_volume", ignoreNulls = true), lit(0)).alias("val_stage_30_50_sand_volume"),
        coalesce(first("val_stage_20_40_sand_volume", ignoreNulls = true), lit(0)).alias("val_stage_20_40_sand_volume")
    )

    val renamedDF = resultDF.withColumnRenamed("data.stage_number_stages", "stage_number_proppants").withColumnRenamed("asset_id_stages", "asset_id_proppants")

    val dfunionfinal = df1
    .join(renamedDF, 
            df1("asset_id_stages") === renamedDF("asset_id_proppants") && 
            df1("`data.stage_number_stages`") === renamedDF("stage_number_proppants"), 
            "left")

    dfunionfinal
    }

// COMMAND ----------

def createUnionColumnfluids(df1: DataFrame, df2: DataFrame): DataFrame = {

   
    val dffluiRe = df2.select(
        col("asset_id").as("asset_id_fluids"),
        col("`data.stage_number`").as("data.stage_number_fluids"),
        col("amount").as("amount_fluids"),
        col("type").as("type_fluids")
        )
    //Insertar filas nuevas val_stage_slickwater_volume y 
    // Hacer el join
    val dfFunion = df1
    .join(dffluiRe, 
            df1("asset_id_stages") === dffluiRe("asset_id_fluids") && 
            df1("`data.stage_number_stages`") === dffluiRe("`data.stage_number_fluids`"), 
            "left") // Mantener todas las filas de df1

    // Rellenar las columnas según el tipo de proppant
    val resultDF = dfFunion
    .withColumn("val_stage_slickwater_volume",
                when(col("type_fluids").contains("Slickwater"), col("amount_fluids")).otherwise(null))
    .withColumn("val_fresh_water",
                when(col("type_fluids").contains("Fresh Water"), col("amount_fluids")).otherwise(null))
    .withColumn("val_flowback_water",
                when(col("type_fluids").contains("Flowback Water"), col("amount_fluids")).otherwise(null))
    .withColumn("val_15hcl_acid",
                when(col("type_fluids").contains("15% HCL Acid"), col("amount_fluids")).otherwise(null))
    .groupBy(df1("`data.stage_number_stages`"), df1("asset_id_stages"))
    .agg(
        coalesce(first("val_stage_slickwater_volume", ignoreNulls = true), lit(0)).alias("val_stage_slickwater_volume"),
        coalesce(first("val_fresh_water", ignoreNulls = true), lit(0)).alias("val_fresh_water"),
        coalesce(first("val_flowback_water", ignoreNulls = true), lit(0)).alias("val_flowback_water"),
        coalesce(first("val_15hcl_acid", ignoreNulls = true), lit(0)).alias("val_15hcl_acid")
    )

    val renamedDF = resultDF.withColumnRenamed("data.stage_number_stages", "stage_number_fluids").withColumnRenamed("asset_id_stages", "asset_id_fluids")
    val dfunionfinal = df1
    .join(renamedDF, 
            df1("asset_id_stages") === renamedDF("asset_id_fluids") && 
            df1("`data.stage_number_stages`") === renamedDF("stage_number_fluids"), 
            "left")
    dfunionfinal
    }

// COMMAND ----------

import org.apache.spark.sql.types.StringType
/**
 * Añade al DataFrame una columna "cod_uwbi_num_stage" con la concatenación
 * de cod_uwbi y num_stage (ambos como String).
 *
 * @param df            DataFrame de entrada.
 * @param codUwbiCol    Nombre de la columna cod_uwbi (por defecto "cod_uwbi").
 * @param numStageCol   Nombre de la columna num_stage (por defecto "num_stage").
 * @param newColName    Nombre de la nueva columna (por defecto "cod_uwbi_num_stage").
 * @return              DataFrame con la columna concatenada añadida.
 */
def addCodUwbiNumStage(
  df: DataFrame,
  codUwbiCol: String     = "cod_uwbi",
  numStageCol: String    = "num_stage",
  newColName: String     = "cod_uwbi_num_stage"
): DataFrame = {
  df.withColumn(
    newColName,
    concat(
        col(numStageCol).cast(StringType),
      col(codUwbiCol).cast(StringType)
    )
  )
}

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

def convertDataTypes(df: DataFrame): DataFrame = {
  df.withColumn("fec_time_start", to_timestamp(col("fec_time_start").cast("long"))) // Convertir long a timestamp

}

// COMMAND ----------

var dlInsert = "N/A"
var dlMsg = ""

var df: DataFrame  = null

try {   
        val dfAssed= transformAssed(getDataFromSource(sourcesFinal("aa001129_usa_assets")))

        val dfwellbore = transformwellb(getDataFromSource(sourcesFinal("eyp0007_tb_dim_d_wellbore_un")))

        val joinWellbAssed = joinAssedWellb(dfAssed,dfwellbore)

        val dfUnionStages = unionStageWellbAssed(joinWellbAssed,getDataFromSource(sourcesFinal("stages")))

        val dfUnionWithproppants = createUnionColumnpropants(dfUnionStages, getDataFromSource(sourcesFinal("proppants")))
        
        val dfUnionwithfluidsandproppants = createUnionColumnfluids(dfUnionWithproppants, getDataFromSource(sourcesFinal("fluids")))

        val dfWithCodvers = Createcodvers(dfUnionwithfluidsandproppants)

        val dfRename = transformationdf(dfWithCodvers)

        val dataframesemiFinal = addCodUwbiNumStage(dfRename, "cod_uwbi", "num_stage", "cod_uwbi_num_stage")

        val dfdf = dataframesemiFinal.select(order.map(col): _*)

        df = addRowDummy(dfdf)

    val dfFormatted = applyDDLFromTableToDF(df,getObject((target_object).toString))
    writeWrapper(sourceDF = dfFormatted, conf = targetFinal, prefix = "target", operationName = target_operation_name, optimize = optimizeDelta)

    dlInsert = "OK"

} catch {
    case e: Throwable => 
                dlMsg = e.toString
                dlInsert = "KO"
                LogHelper().logEndKO(dlMsg)
}


println(dlInsert)
println(dlMsg)

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC # Informar del resultado

// COMMAND ----------


val result = s"""{"error": "$error","dlInsert": { "result": "$dlInsert", "msg": "$dlMsg" }}"""

if (uniqueKey.nonEmpty && step > 0)
    ControlHelper().updateControlTable(spark, uniqueKey, step, s"$name: $result")

if (!error.isEmpty || dlInsert.equals("KO")) {
    LogHelper().logError(s"$name: $result")
    throw new Exception(s"$name: $result")
} 

LogHelper().logInfo(s"$name: $result")
dbutils.notebook.exit(result)  

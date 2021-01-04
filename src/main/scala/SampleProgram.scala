
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import samples.DataFrameReader
import samples.config.JsonConfigProtocol.jsonConfig
import samples.config.{ConfigReader, JsonConfig}
import samples.utils.DataFrameSchema
import spray.json._


object SampleProgram {

  def main(args: Array[String]): Unit = {

    //Exercice 1
    Logger.getLogger("org").setLevel(Level.OFF)

    implicit val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Question 1 : Lire tous ces fichiers sous forme d'un seul Dataframe (ou d'un Dataset, au choix)

    val schema = StructType(Seq(
      StructField("amount", IntegerType),
      StructField("base_currency", StringType),
      StructField("currency", StringType),
      StructField("exchange_rate", DoubleType)
    ))

    val df: DataFrame = sparkSession.read.format("csv").option("delimiter", ",").schema(schema).option("header", false).load("input_data")

    //Question 2 : Rajouter une colonne contenant la date du fichier duquel vient chacune des lignes

    sparkSession.udf.register("get_file_name", (path: String) => path.split("/").last.split("\\.").head)

    val dates_df = df.withColumn("date", callUDF("get_file_name", input_file_name()))
    dates_df.show()

    //Question 3 : Remplir les gaps comme décrit précédemment
    val w = Window.partitionBy("date").orderBy("date")

    val iterations = dates_df.withColumn("iter", row_number over w)

    val counter = iterations.join(iterations.groupBy("date").agg(count("date").as("complet")), "date")

    counter.show()

    val fillWithDaysAgo = 1
    val inconsistents = counter
      .filter("complet < 8")
      //.filter(col("iter") < col("complet"))
      //.drop("iter", "complet")
      .selectExpr("date as dateMissing", "date_add(date, - " + fillWithDaysAgo + ") as previousDate", "iter", "complet")

    inconsistents.show()

    val missings = dates_df
      .join(inconsistents, inconsistents("previousDate") === dates_df("date"), "cross")
      .drop("date")

    missings.show()
    //df_date.union(row)
    dates_df.orderBy("date").show()
    //df_date.write.partitionBy("date").mode(SaveMode.Overwrite).parquet("export")

    //Question 4 : Écrire les résultats en partitionnant par date (la colonne que nous avons rajouté dans la question 2.)

    dates_df.write.partitionBy("date").mode(SaveMode.Overwrite).parquet("output_data_1")
    //
    //Exercice 2

    // Question 1 : Automatiser le schéma avec config json
    val confReader = ConfigReader.readConfig("conf/config.json")
    val configuration = confReader.parseJson.convertTo[JsonConfig]

    val dfSchema: StructType = DataFrameSchema.buildDataframeSchema(configuration.fields)
    val data = DataFrameReader.readCsv("input_data", configuration.csvOptions, dfSchema)

    data.printSchema()
    data.show

    // Question 2 :
    data.write.partitionBy("date").mode(SaveMode.Overwrite).parquet("output_data_2")
  }
}

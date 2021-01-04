package samples.utils

import org.apache.spark
import org.apache.spark.sql.types._
import samples.config.Field

//Objet qui relie le fichier json à notre fonction buildDataframe

object DataFrameSchema {

  def buildDataframeSchema(fields: Seq[Field]): StructType = {
    val structFieldsList = fields.map(field => {
      println(field)
      spark.sql.types.StructField(field.date, mapPrimitiveTypesToSparkTypes(field.fillWithDaysAgo))
    })

    spark.sql.types.StructType(structFieldsList)
  }

//On associe les types au format attendu en scala

  def mapPrimitiveTypesToSparkTypes[T](fillWithDaysAgo:T): DataType = {
    fillWithDaysAgo match {
      case _: String => StringType
      case _: Double => DoubleType
      case _: Long => LongType
      case _: Int => IntegerType
      case _ => StringType
    }
  }
}

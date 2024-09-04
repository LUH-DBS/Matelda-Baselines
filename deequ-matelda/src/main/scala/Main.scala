import com.amazon.deequ.analyzers.DataTypeInstances
import com.amazon.deequ.profiles.{NumericColumnProfile, StringColumnProfile}
import com.amazon.deequ.schema.{IntColumnDefinition, RowLevelSchema, RowLevelSchemaValidator}
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DecimalType

import java.io.File

object Main {
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .master("local")
      .appName("deequ-matelda")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))

    val data_base_path = "/Users/fatemehahmadi/IdeaProjects/deequ-matelda/datasets/REIN_retry"

    new File(data_base_path).listFiles.foreach(dataset_base_path => {

      val clean_data_path = s"${dataset_base_path.getAbsolutePath}/clean.csv"
      val dirty_data_path = s"${dataset_base_path.getAbsolutePath}/dirty.csv"
      val clean_df = session.read.option("header", "true").option("inferSchema", "true").csv(clean_data_path)
      var dirty_df = session.read.option("header", "true").option("inferSchema", "true").csv(dirty_data_path)
//      dirty_df = dirty_df.withColumnsRenamed(dirty_df.columns.zip(clean_df.columns).toMap)

      println(s"Start suggestion for dataset: ${clean_data_path}")
      val suggestionResult = ConstraintSuggestionRunner()
        .onData(clean_df)
        .addConstraintRules(Rules.EXTENDED)
        .run()

      var schema = RowLevelSchema()
      suggestionResult.columnProfiles.foreach { case (column, profile) =>
        println(s"Profile for column '$column': $profile")
        profile.dataType match {
          case DataTypeInstances.String =>
            val castedProfile = profile.asInstanceOf[StringColumnProfile]
            schema = schema.withStringColumn(column, castedProfile.completeness != 1, castedProfile.minLength, castedProfile.maxLength)
          case DataTypeInstances.Integral =>
            val castedProfile = profile.asInstanceOf[NumericColumnProfile]
            schema = schema.withIntColumn(column, castedProfile.completeness != 1, castedProfile.minimum.map(_.toInt), castedProfile.maximum.map(_.toInt))
          case DataTypeInstances.Fractional =>
            val castedProfile = profile.asInstanceOf[NumericColumnProfile]
            schema = schema.withDecimalColumn(column, DecimalType.MAX_PRECISION, DecimalType.MAX_SCALE, castedProfile.completeness != 1)
          case _ =>
            println("Unknown")
        }
      }

      var resultDf: sql.DataFrame = null
      for (colDefinition <- schema.columnDefinitions) {
        val indexColName = "matelda_index"
        val colName = colDefinition.name
        val colSchema = RowLevelSchema(Seq(colDefinition)).withIntColumn(indexColName)
        val colResult = RowLevelSchemaValidator.validate(dirty_df.select(indexColName, colName), colSchema)
        if (colResult.numInvalidRows > 0) {
          println(s"Errors for $colName: ${colResult.numInvalidRows}")
          println(s"Reason: $colDefinition")
          val colResultDf = colResult.invalidRows.withColumn("col", lit(colName)).select("matelda_index", "col")
          if (resultDf == null) {
            resultDf = colResultDf
          } else {
            resultDf = resultDf.union(colResultDf)
          }
        } else {
          println(s"No errors for $colName")
        }
      }
      if (resultDf == null) {
        println("No errors found")
      } else {
        resultDf.coalesce(1).write.option("header", "true").mode("overwrite").csv(s"${dataset_base_path.getAbsolutePath}/result_clean")
      }
    })




    //    val result = RowLevelSchemaValidator.validate(dirty_df, schema)
    //
    //    println(schema)
    //    result.invalidRows.show(100)

    session.stop()
    System.clearProperty("spark.driver.port")
  }
}
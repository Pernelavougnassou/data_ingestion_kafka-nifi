package Institut.Superieur.Management.Services

import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {
  def readCsv(spark: SparkSession, header: String, delimiter: String, path: String) = {
    spark.read.format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(path)
  }

  def saveAsTable(df: DataFrame, dataBaseName: String,
                  tableName: String, path: String, mode: String,
                  format: String) = {
    df.write
      .format(s"$format") // format = parquet | orc ...
      .mode(s"$mode") // mode = append | overwrite | ignore | errorifexists
      .option("path", s"$path")
      .saveAsTable(s"$dataBaseName.$tableName")
  }
}

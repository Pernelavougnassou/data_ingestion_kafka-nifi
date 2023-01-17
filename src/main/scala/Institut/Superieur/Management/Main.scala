package Institut.Superieur.Management

import Institut.Superieur.Management.Services.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, substring, sum}
import org.apache.spark.sql.types.{DateType, StringType, StructType}
import java.io.File
import org.apache.spark.streaming.{Minutes, StreamingContext}

object Main {

  /**
   *
   * @param args
   */

  def main(args: Array[String]): Unit = {
    // create spark session with
    val pathCV = args(0)

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder
      .appName("kafka_hive")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    // create kafka streaming data frame (each 15 minutes)
    //val streaming = new StreamingContext(spark.sparkContext, Minutes(15))
    val kafkaDS = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "m10223.contaboserver.net:6667")
      .option("subscribe", "ism2022_m2_pernelAvougnassou_covid")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    //streaming.start()
    //streaming.awaitTermination()

    val schema = new StructType()
      .add("date_cas", DateType, true)
      .add("variant", StringType, true)
      .add("nb_cas", StringType, true)
      .add("id_pays", StringType, true)

    val new_df = kafkaDS.withColumn("struct_value", from_json(col("value"), schema))
    new_df
      .writeStream
      .outputMode("update")
      .option("truncate", false)
      .format("console")
      .start()
      .awaitTermination()

    // read pays_covid file
    val pays_covid = Utils.readCsv(spark,
      "true",
      ",",
      pathCV)

    // Get the wording by join pays_covid file
    val join_country = new_df.join(pays_covid, Seq("id_pays"), "inner")

    // Drop the Column non_Who
    val variantWithoutNonWho = join_country.filter(col("variant") =!= "non_who")

    // Get the number of covid by year and country
    val column_annee = variantWithoutNonWho.withColumn("annee_cas", substring(col("date_cas"), 0, 4))
    val casParAnnee = column_annee
      .groupBy("id_pays", "annee_cas")
      .agg(sum("nb_cas").as("total_covid_par_annee"))

    // Write on Hive
    Utils.saveAsTable(casParAnnee,
      "ism_m2_2022_examspark",
      "spark_stream_pernel",
      "/user/ism_student4/m2_bigdata_2022/Pernel/exam/output",
      "overwrite",
      "parquet")
  }
}

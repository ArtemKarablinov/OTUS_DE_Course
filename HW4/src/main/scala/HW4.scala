package scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object HW4 extends App {

    val spark = SparkSession
        .builder
        .appName("WordCount")
        .getOrCreate()


    val sc = spark.sparkContext

    import spark.implicits._

    val lines = spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 666)
        .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
        .outputMode("complete")
        .format("console")
        .option("checkpointLocation", "HW4/checkpoint")
        .start()

    query.awaitTermination()

}
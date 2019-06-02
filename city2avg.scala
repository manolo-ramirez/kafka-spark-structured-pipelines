package com.manoloramirez.sparkkafka1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object City2Avg {

  def main(args: Array[String]): Unit = {
    // We create the spark session
    val spark = SparkSession
      .builder
      .appName("StructuredCityToAverage")
      .config("spark.master", "local")
      .getOrCreate()

    import spark.implicits._

    // We read from kafka as a continuously updating dataframe
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "postgre-json-city")
      //.option("failOnDataLoss", "true")
      .option("startingOffsets", "earliest")
      .load()

    // We extract the json schema of the value in kafka from a small file where we have one json record
    val smallBatchSchema = spark.read.json("file:///home/manolo/schema.txt").schema

    // We arrange the dataframe to show columns $time, which is a timestamp, as well as $id, $name, $country_code,
    // $district and $population
    val dataDF = df.selectExpr("CAST(value AS STRING) as value_json", "timestamp as timestamp")
      .select("timestamp", "value_json")
      .withColumn("time", unix_timestamp($"timestamp", "yyyy-MM-dd HH:mm:ss.SS").cast(TimestampType))
      .withColumn( "data", from_json($"value_json", schema=smallBatchSchema))
      .withColumn("id", $"data.payload.id")
      .withColumn("name", $"data.payload.name")
      .withColumn("country_code", $"data.payload.countrycode")
      .withColumn("district", $"data.payload.district")
      .withColumn("population", $"data.payload.population")
      .drop("timestamp").drop("value_json").drop("data")

    // We find out the average population per city for each country code by using watermarking
    val windowedAvgDF = dataDF.withWatermark("time", "10 seconds")
      .groupBy( window($"time", "5 second", "1 second"),
      col("country_code"))
      .agg(avg("population").as("avg_population")).drop("window")

    // finally, we show the averaged result on console
    val  query1 = windowedAvgDF.writeStream
      .outputMode("append")
      .format("console")
      .start()

    query1.awaitTermination()

  }
}

package com.github.anicolaspp.spark.sql.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object App {


  def main(args: Array[String]): Unit = {

    val config = new org.apache.spark.SparkConf().setAppName("testing streaming")

    val sparkSession = SparkSession
      .builder()
      .config(config)
      .getOrCreate()

    sparkSession.conf.set("spark.sql.streaming.checkpointLocation", "/Users/nperez/check")
    sparkSession.conf.set("spark.sql.streaming.schemaInference", value = true)
    sparkSession.sparkContext.setLogLevel("WARN")

    val s = StructType(List(StructField("value", StringType), StructField("ts", LongType)))


    val r = sparkSession
      .readStream
      .format("com.github.anicolaspp.spark.sql.streaming.DefaultSource")
      .schema(s)
      .load()

    r.createTempView("w")

    sparkSession
      .sql("select ts, count(*) as c from w group by ts order by ts, c desc")
      .writeStream
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()
      .awaitTermination()
  }
}

case class T(value: String, ts: String)
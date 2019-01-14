package com.github.anicolaspp.spark.sql.streaming

import org.apache.spark.sql.anicolaspp.{InMemoryRandomStrings, InMemoryRandomStrings}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

class DefaultSource extends StreamSourceProvider with DataSourceRegister with StreamSinkProvider {

  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    
    (shortName(), InMemoryRandomStrings.schema)
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {
    
    new InMemoryRandomStrings(sqlContext)
  }

  override def shortName(): String = "InMemoryRandomStrings"

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = new Sink {
    override def addBatch(batchId: Long, data: DataFrame): Unit = {

      println(batchId)

      data.collect().foreach(println)
    }
  }
}
package org.apache.spark.sql.anicolaspp

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String

import scala.util.Random

class InMemoryRandomStrings private (sqlContext: SQLContext) extends Source {
  private var offset: LongOffset = LongOffset(-1)

  private var batches = collection.mutable.ListBuffer.empty[(String, Long)]

  private val incrementalThread = dataGeneratorStartingThread()

  override def schema: StructType = InMemoryRandomStrings.schema

  override def getOffset: Option[Offset] = this.synchronized {
    println(s"getOffset: $offset")

    if (offset.offset == -1) None else Some(offset)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = this.synchronized {

    val s = start.flatMap(LongOffset.convert).getOrElse(LongOffset(-1)).offset + 1
    val e = LongOffset.convert(end).getOrElse(LongOffset(-1)).offset + 1

    println(s"generating batch range $start ; $end")
    
    val data = batches
      .par
      .filter { case (_, idx) => idx >= s && idx <= e }
      .map { case (v, _) => (v, v.length) }
      .seq

    val rdd = sqlContext
      .sparkContext
      .parallelize(data)
      .map { case (v, l) => InternalRow(UTF8String.fromString(v), l.toLong) }

    sqlContext.sparkSession.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }

  override def commit(end: Offset): Unit = this.synchronized {

    val committed = LongOffset.convert(end).getOrElse(LongOffset(-1)).offset

    val toKeep = batches.filter { case (_, idx) => idx > committed }

    println(s"after clean size ${toKeep.length}")

    println(s"deleted: ${batches.size - toKeep.size}")

    batches = toKeep
  }

  override def stop(): Unit = incrementalThread.stop()

  private def dataGeneratorStartingThread() = {
    val t = new Thread("increment") {
      setDaemon(true)
      override def run(): Unit = {

        while (true) {
          try {
            this.synchronized {
              offset = offset + 1

              val value = Random.nextString(Random.nextInt(5))

              batches.append((value, offset.offset))
            }
          } catch {
            case e: Exception => println(e)
          }

          Thread.sleep(100)
        }
      }

    }

    t.start()

    t
  }
}

object InMemoryRandomStrings {

  def apply(sqlContext: SQLContext): Source = new InMemoryRandomStrings(sqlContext)

  lazy val schema = StructType(List(StructField("value", StringType), StructField("ts", LongType)))
}
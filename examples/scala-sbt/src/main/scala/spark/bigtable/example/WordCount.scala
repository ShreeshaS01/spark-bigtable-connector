/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark.bigtable.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
import java.nio.ByteBuffer

object WordCount extends App {
  val (projectId, instanceId, tableName, createNewTable) = parse(args)

  val spark = SparkSession.builder().getOrCreate()

  val doubleToBinaryUdf = udf((value: Double) => doubleToBinary(value))
  val binaryToDoubleUdf =
    udf((binaryData: Array[Byte]) => binaryToDouble(binaryData))

  val catalog =
    s"""{
       |"table":{"namespace":"default", "name":"$tableName", "tableCoder":"PrimitiveType"},
       |"rowkey":"wordCol",
       |"columns":{
       |  "word":{"cf":"rowkey", "col":"wordCol", "type":"string"},
       |  "count":{"cf":"example_family", "col":"countCol", "type":"long"},
       |  "frequency_binary":{"cf":"example_family", "col":"frequencyCol", "type":"binary"}
       |}
       |}""".stripMargin

  import spark.implicits._
  val data = (1 to 9999999).map(i => ("word%d".format(i%3000000), i, i / 9999999.0))
  val rdd = spark.sparkContext.parallelize(data)
  val dfWithDouble = rdd.toDF("word", "count", "frequency_double")

  val customData = (1 to 1000).map(i => ("word%d".format(i), i + 1, i + 1 / 1000.0))
  val customRdd = spark.sparkContext.parallelize(customData)
  val customDf = customRdd.toDF("word", "count", "frequency_double")

  println("Created the DataFrame:");
  dfWithDouble.show()

  val dfToWrite = dfWithDouble
    .withColumn(
      "frequency_binary",
      doubleToBinaryUdf(dfWithDouble.col("frequency_double"))
    )
    .drop("frequency_double")
  println(dfToWrite.rdd.getNumPartitions)
  dfToWrite.write
    .format("bigtable")
    .option("catalog", catalog)
    .option("spark.bigtable.project.id", projectId)
    .option("spark.bigtable.instance.id", instanceId)
    .option("spark.bigtable.create.new.table", createNewTable)
    .save
  println("DataFrame was written to Bigtable.")

  val readDf = spark.read
    .format("bigtable")
    .option("catalog", catalog)
    .option("spark.bigtable.project.id", projectId)
    .option("spark.bigtable.instance.id", instanceId)
    .load

  val readDfWithDouble = readDf
    .withColumn(
      "frequency_double",
      binaryToDoubleUdf(readDf.col("frequency_binary"))
    )
    .drop("frequency_binary")

  println("Reading the DataFrame from Bigtable:");
  println(readDfWithDouble.rdd.getNumPartitions)
  readDfWithDouble.show()

  def parse(args: Array[String]): (String, String, String, String) = {
    import scala.util.Try
    val projectId = Try(args(0)).getOrElse {
      throw new IllegalArgumentException(
        "Missing command-line argument: SPARK_BIGTABLE_PROJECT_ID"
      )
    }
    val instanceId = Try(args(1)).getOrElse {
      throw new IllegalArgumentException(
        "Missing command-line argument: SPARK_BIGTABLE_INSTANCE_ID"
      )
    }
    val tableName = Try(args(2)).getOrElse {
      throw new IllegalArgumentException(
        "Missing command-line argument: SPARK_BIGTABLE_TABLE_NAME"
      )
    }
    val createNewTable = Try(args(3)).getOrElse {
      "true"
    }
    (projectId, instanceId, tableName, createNewTable)
  }

  def doubleToBinary(value: Double): Array[Byte] = {
    ByteBuffer.allocate(java.lang.Double.BYTES).putDouble(value).array()
  }

  def binaryToDouble(binaryData: Array[Byte]): Double = {
    if (binaryData == null || binaryData.length == 0) {
      Double.NaN
    } else {
      ByteBuffer.wrap(binaryData).getDouble()
    }
  }
}

object JoinTest {
  /*
  * customDf suppose to be a small df (kind of subset of a lookup table)
  * bigtableDf is a very big table compared to the custom df
  * default joining column is rowkey column
  *
  * 1. perform join with full table scan
  * 2. perform join with partial table scan by filtering only required record
  * 3. Note down the time
  *
  * */
  def executeFullScanInnerJoin(customDf: DataFrame, bigtableDf: DataFrame, onColumn: String): Unit = {
    val startTime = System.currentTimeMillis()
    val resDf = customDf.join(bigtableDf, onColumn)
    val count = resDf.count
    val endTime = System.currentTimeMillis()
    println(s"executeFullScanInnerJoin: ${(endTime - startTime) / 1000}\n count=$count")
  }

  def executePartialScanInnerJoin(customDf: DataFrame, bigtableDf: DataFrame, onColumn: String): Unit = {
    val startTime = System.currentTimeMillis()

    val dfArray: Array[DataFrame] = Array[DataFrame]()
    customDf.select(onColumn).rdd.foreachPartition { iterator =>
      val rowkeyValueList = iterator.map { row => row.get(0).toString }.toSeq
      val bigtableDataset = bigtableDf.where(col(onColumn).isInCollection(rowkeyValueList))
      dfArray.appended(bigtableDataset)
    }
    val requiredBigtableDf = dfArray.reduce(_ union _)
    val resDf = customDf.join(requiredBigtableDf, onColumn)
    val count = resDf.count
    val endTime = System.currentTimeMillis()
    println(s"executeFullScanInnerJoin: ${(endTime - startTime) / 1000}\n count=$count")
  }
}

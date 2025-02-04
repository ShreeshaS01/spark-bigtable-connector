package com.google.cloud.spark.bigtable.join

import com.google.cloud.spark.bigtable.datasources._
import com.google.cloud.spark.bigtable.filters.SparkSqlFilterAdapter
import com.google.cloud.spark.bigtable.{ReadRowConversions, UserAgentInformation}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.{EqualTo, Filter, Or}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, Row => SparkRow}

object BigtableJoinImplicit extends Serializable {

  println("Hello")

  implicit class DataFrameRich(df: DataFrame) extends Serializable {
    /*
     * Map(
     *   "catalog" -> "",
     *   "joinKey" -> "col1,col2",
     *   "joinType" -> "inner",
     *   "requiredColumns" -> "col1,col2,col3"
     * )
     * */

    private def buildScan(
        requiredColumns: Array[String],
        filters: Array[Filter],
        catalog: BigtableTableCatalog,
        pushDownRowKeyFilters: Boolean,
        clientKey: BigtableClientKey,
        startTimestampMicros: Option[Long],
        endTimestampMicros: Option[Long]
    ): Iterator[SparkRow] = {
      val filterRangeSet =
        SparkSqlFilterAdapter.createRowKeyRangeSet(filters, catalog, pushDownRowKeyFilters)
      val scanner: BigtableTableScan =
        new BigtableTableScan(
          clientKey,
          filterRangeSet,
          catalog.name,
          startTimestampMicros,
          endTimestampMicros
        )

      val fieldsOrdered = requiredColumns.map(catalog.sMap.getField)
      val partitions = scanner.getPartitions
      val rowIteratorArr = partitions.map(scanner.compute)
      rowIteratorArr.toIterator.flatMap { itr =>
        itr.map(r => ReadRowConversions.buildRow(fieldsOrdered, r, catalog))
      }
    }

    def joinWithBigtable(
        parameters: Map[String, String]
    )(implicit spark: SparkSession): DataFrame = {
      val joinCols = parameters("joinKey").split(",").map(x => x.trim)
      val requiredColumns = parameters("requiredColumns").split(",").map(x => x.trim)
      val joinType = parameters("joinType")
      val catalog: BigtableTableCatalog = BigtableTableCatalog(parameters)
      val structFields = requiredColumns.map { x =>
        val field = catalog.sMap.getField(x)
        StructField(field.sparkColName, field.dt)
      }
      val bigtableSchema = StructType(structFields)
      val bigtableSparkConf: BigtableSparkConf =
        BigtableSparkConfBuilder().fromMap(parameters).build()
      val pushDownRowKeyFilters = bigtableSparkConf.pushDownRowKeyFilters
      val startTimestampMicros =
        bigtableSparkConf.timeRangeStart.map(timestamp => Math.multiplyExact(timestamp, 1000L))
      val endTimestampMicros =
        bigtableSparkConf.timeRangeEnd.map(timestamp => Math.multiplyExact(timestamp, 1000L))
      val clientKey = new BigtableClientKey(bigtableSparkConf, UserAgentInformation.RDD_TEXT)

      val bigtableRdd = df.select(joinCols.map(col): _*).rdd.mapPartitions { p =>
        val values = p.map(r => r.get(0)).toArray
        val filters: Array[Filter] = values.map(v => EqualTo("word", v))
        val orFilter: Array[Filter] = Array(filters.reduce(Or))
        buildScan(
          requiredColumns = requiredColumns,
          filters = orFilter,
          catalog = catalog,
          pushDownRowKeyFilters = pushDownRowKeyFilters,
          clientKey = clientKey,
          startTimestampMicros = startTimestampMicros,
          endTimestampMicros = endTimestampMicros
        )
      }
      bigtableRdd.foreachPartition(itr => {
        println(itr.map(it => it.get(0)).mkString(","))
        println("\n")
      })
      val bigtableDf = spark.createDataFrame(bigtableRdd, bigtableSchema)
      df.join(bigtableDf, joinCols, joinType)
    }
  }
}

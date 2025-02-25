package com.google.cloud.spark.bigtable.join

import com.google.cloud.spark.bigtable.datasources._
import com.google.cloud.spark.bigtable.{Logging, ReadRowConversions, UserAgentInformation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.google.api.gax.rpc.ServerStream
import com.google.cloud.bigtable.data.v2.models.{Query, TableId, Row => BigtableRow}
import com.google.protobuf.ByteString

object BigtableJoinImplicit extends Serializable with Logging {

  implicit class RichDataFrame(leftDf: DataFrame) extends Serializable with Logging {
    private val JOIN_KEY = "join.key"
    private val JOIN_TYPE = "join.type"
    private val COLUMNS_REQUIRED = "columns.required"
    private val JOIN_STRATEGY = "join.strategy"
    private val PARTITION_COUNT = "partition.count"
    private val BATCH_ROWKEY_SIZE = "batch.rowKeySize"
    private val INNER = "inner"
    private val SHUFFLE = "shuffle"
    private val MAP = "map"
    private val MAP_SORTMERGE = "map.sortmerge"
    private val MAP_HASHMERGE = "map.hashmerge"
    private val DEFAULT_BATCH_SIZE = "25000"

    /** Joins a DataFrame with Bigtable based on the provided parameters.
      *
      * This function fetches data from Bigtable and joins it with a source DataFrame using either
      * a **shuffle join** (default) or a **map-side join** (if specified). The join type can be "inner" or "left".
      *
      * ### Example Usage:
      * {{{
      * val joinConfig: Map[String, String] = Map(
      *     "spark.bigtable.project.id" -> projectId,
      *     "spark.bigtable.instance.id" -> instanceId,
      *     "catalog" -> catalog,
      *     "join.key" -> "order_id",
      *     "join.type" -> "inner",  // Options: "inner", "left" (default: "inner")
      *     "columns.required" -> "order_id,product_id,amount",
      *     "join.strategy" -> "map.sortMerge",  // Options: "map.sortMerge" (default for "map"), "map.hashMerge", "shuffle" (default)
      *     "partition.count" -> "1000",
      *     "batch.rowKeySize" -> "10000"  // Default batch size
      * )
      * val joinedDf = df.joinWithBigtable(joinConfig)
      * }}}
      *
      * @param params Configuration parameters for the Bigtable join.
      * @param spark  Implicit Spark session.
      * @return       A DataFrame resulting from the join operation.
      */
    def joinWithBigtable(
        params: Map[String, String]
    )(implicit spark: SparkSession): DataFrame = {

      // Extract required parameters
      val joinColumns = params(JOIN_KEY).split(",").map(_.trim)
      val requiredColumns = extractRequiredColumns(params)
      val joinType = params.getOrElse(JOIN_TYPE, INNER).toLowerCase
      val partitionNumber = params
        .get(PARTITION_COUNT)
        .map(_.toInt)
        .getOrElse(leftDf.rdd.getNumPartitions)

      // Create Bigtable catalog and schema
      val catalog = BigtableTableCatalog(params)
      val bigtableSchema = constructBigtableSchema(catalog, requiredColumns, joinColumns)

      // Repartition source DataFrame by join key(s) for better performance
      // TODO: if leftDf is already range partitioned it is not needed
      val sortedSrcDf = leftDf.repartitionByRange(partitionNumber, joinColumns.map(col): _*)

      // Determine the join strategy
      val joinStrategy = params.getOrElse(JOIN_STRATEGY, SHUFFLE)
      logInfo(s"joinStrategy ==> $joinStrategy")
      joinStrategy.toLowerCase match {
        case MAP | MAP_SORTMERGE | MAP_HASHMERGE =>
          bigtableJoin(params, sortedSrcDf, joinColumns, bigtableSchema, catalog)
        case _ =>
          val btDf = fetchBigtableData(params, sortedSrcDf, joinColumns, bigtableSchema, catalog)
          sortedSrcDf.join(btDf, joinColumns, joinType)
      }
    }

    /** Extracts the required column names from parameters, returns an empty array if none are specified. */
    private def extractRequiredColumns(parameters: Map[String, String]): Array[String] = {
      parameters.getOrElse(COLUMNS_REQUIRED, "").split(",").map(_.trim).filter(_.nonEmpty)
    }

    /** Constructs the schema for Bigtable based on the required columns and join columns.
      *
      * @param catalog        The Bigtable catalog containing schema mappings.
      * @param requiredColumns Array of columns required in the output schema.
      * @param joinColumns    Array of columns used for joining.
      * @return StructType representing the final schema.
      */
    private def constructBigtableSchema(
        catalog: BigtableTableCatalog,
        requiredColumns: Array[String],
        joinColumns: Array[String]
    ): StructType = {
      // If requiredColumns is provided, prioritize its order
      val finalColumnList = if (requiredColumns.nonEmpty) {
        joinColumns ++ requiredColumns.filterNot(joinColumns.contains)
      } else {
        val allFields = catalog.sMap.toFields.map(_.name)
        joinColumns ++ allFields.filterNot(joinColumns.contains)
      }
      // Map column names to StructField using the catalog schema
      val structFields = finalColumnList.map { columnName =>
        val field = catalog.sMap.getField(columnName)
        StructField(field.sparkColName, field.dt)
      }

      StructType(structFields)
    }

    /** Fetches data from Bigtable for the given DataFrame's join keys.
      *
      * This method extracts row keys from the source DataFrame, queries Bigtable in batches,
      * and returns the matching rows as a DataFrame.
      *
      * @param parameters     Configuration map for Bigtable connection and batch settings.
      * @param sortedSrcDf    Source DataFrame, pre-sorted by join keys.
      * @param joinColumns    Columns used as row keys for lookup in Bigtable.
      * @param bigtableSchema Expected schema of the Bigtable data.
      * @param catalog        Bigtable table catalog containing schema mappings.
      * @param spark          Implicit Spark session.
      * @return               DataFrame with data fetched from Bigtable.
      */
    private def fetchBigtableData(
        parameters: Map[String, String],
        sortedSrcDf: DataFrame,
        joinColumns: Array[String],
        bigtableSchema: StructType,
        catalog: BigtableTableCatalog
    )(implicit spark: SparkSession): DataFrame = {

      // Extract required column names and configure Bigtable client
      val (clientKey, orderedFields, batchSize) =
        getBigtableConfig(parameters, bigtableSchema, catalog)

      // Fetch Bigtable rows based on join keys from the source DataFrame
      val bigtableRdd = sortedSrcDf
        .select(joinColumns.map(col): _*)
        .rdd
        .mapPartitionsWithIndex { (partitionIndex, rows) =>
          val rowKeys = rows.map(_.getString(0)).toArray
          val bigtableRows =
            fetchBigtableRows(rowKeys, clientKey, catalog.name, partitionIndex, batchSize)
          bigtableRows.map(row => ReadRowConversions.buildRow(orderedFields, row, catalog))
        }

      // Convert RDD to DataFrame and return
      spark.createDataFrame(bigtableRdd, bigtableSchema)
    }

    /** Joins a sorted DataFrame (`sortedSrcDf`) with Bigtable data using a user-defined join strategy.
      *
      * @param parameters     Bigtable connection parameters.
      * @param sortedSrcDf    Source DataFrame, pre-sorted by join keys.
      * @param joinColumns    Columns used for joining.
      * @param bigtableSchema Schema of the Bigtable data.
      * @param catalog        Bigtable table catalog for schema mapping.
      * @param spark          Implicit SparkSession.
      * @return Joined DataFrame.
      */
    private def bigtableJoin(
        parameters: Map[String, String],
        sortedSrcDf: DataFrame,
        joinColumns: Array[String],
        bigtableSchema: StructType,
        catalog: BigtableTableCatalog
    )(implicit spark: SparkSession): DataFrame = {

      val (clientKey, orderedFields, batchSize) =
        getBigtableConfig(parameters, bigtableSchema, catalog)
      val joinStrategy = parameters.getOrElse(JOIN_STRATEGY, MAP_SORTMERGE).toLowerCase

      val bigtableRdd = sortedSrcDf.rdd
        .mapPartitionsWithIndex { case (i, partition) =>
          val srcRows = partition.toArray
          val rowKeys = srcRows.map(_.getString(0))

          val btRows = fetchBigtableRows(rowKeys, clientKey, catalog.name, i, batchSize)
            .map(row => ReadRowConversions.buildRow(orderedFields, row, catalog))
            .toArray

          joinStrategy match {
            case MAP_HASHMERGE => hashMergeJoin(srcRows, btRows, joinColumns)
            case _             => sortMergeJoin(srcRows, btRows, joinColumns)
          }
        }

      val finalSchema = getCombinedSchema(sortedSrcDf.schema, bigtableSchema, joinColumns)
      spark.createDataFrame(bigtableRdd, finalSchema)
    }

    /** Extracts and initializes the Bigtable client configuration.
      *
      * @param parameters Configuration parameters for Bigtable.
      * @param bigtableSchema Schema of the Bigtable table.
      * @param catalog Bigtable catalog containing schema mappings.
      * @return A tuple containing:
      *         - BigtableClientKey for authentication and interaction.
      *         - Array of ordered fields mapped from the schema.
      *         - Batch size for row key processing.
      */
    private def getBigtableConfig(
        parameters: Map[String, String],
        bigtableSchema: StructType,
        catalog: BigtableTableCatalog
    ): (BigtableClientKey, Array[Field], Int) = {
      val requiredCols = bigtableSchema.map(_.name).toArray
      val btConfig = BigtableSparkConfBuilder().fromMap(parameters).build()
      val clientKey = new BigtableClientKey(btConfig, UserAgentInformation.RDD_TEXT)
      val orderedFields = requiredCols.map(catalog.sMap.getField)
      val batchSize = parameters.getOrElse(BATCH_ROWKEY_SIZE, DEFAULT_BATCH_SIZE).toInt
      (clientKey, orderedFields, batchSize)
    }

    /** Performs a hash-based merge join between two arrays of Rows.
      *
      * @param srcRows Source rows from the primary dataset.
      * @param btRows  Rows fetched from Bigtable.
      * @param keyArr  Array of column names used as joining keys.
      * @return An iterator of joined rows.
      */
    private def hashMergeJoin(
        srcRows: Array[Row],
        btRows: Array[Row],
        keyArr: Array[String]
    ): Iterator[Row] = {
      val joiningKeysWithIndex = keyArr.map(_.asInstanceOf[Any]).zipWithIndex
      val hashRowKeyMap = btRows.map { row =>
        val key = joiningKeysWithIndex.map { case (_, ki) => row.getString(ki) }.mkString
        (key, row)
      }.toMap
      val joinedRows = srcRows.map { row =>
        val key = joiningKeysWithIndex.map { case (_, ki) => row.getString(ki) }.mkString
        val btValues = hashRowKeyMap(key).toSeq.drop(keyArr.length)
        Row.fromSeq(row.toSeq ++ btValues)
      }
      joinedRows.iterator
    }

    /** Performs a sort-merge join between two arrays of rows based on the given join keys.
      *
      * @param srcRows Source table rows.
      * @param sortedBtRows  Bigtable rows.
      * @param keys    Join key column names.
      * @return        Iterator of joined rows.
      */
    private def sortMergeJoin(
        srcRows: Array[Row],
        sortedBtRows: Array[Row],
        keys: Array[String]
    ): Iterator[Row] = {

      val result = scala.collection.mutable.ListBuffer[Row]()
      val keyIndices = keys.map(_.asInstanceOf[Any]).zipWithIndex

      // Sort both arrays based on join keys
      val sortedSrc = srcRows
        .sortBy(row => keyIndices.map { case (_, idx) => row.getString(idx) }.mkString)

      var (srcIdx, btIdx) = (0, 0)

      while (srcIdx < sortedSrc.length && btIdx < sortedBtRows.length) {
        val srcKey = keyIndices.map { case (_, idx) => sortedSrc(srcIdx).getString(idx) }.mkString
        val btKey = keyIndices.map { case (_, idx) => sortedBtRows(btIdx).getString(idx) }.mkString

        if (srcKey < btKey) srcIdx += 1
        else if (srcKey > btKey) btIdx += 1
        else {
          val btValues =
            sortedBtRows(btIdx).toSeq.drop(keys.length) // Drop join keys from Bigtable row
          result += Row.fromSeq(sortedSrc(srcIdx).toSeq ++ btValues)
          srcIdx += 1
          btIdx += 1
        }
      }

      result.iterator
    }

    /** Generates a combined schema by merging the source schema with the Bigtable schema.
      *
      * @param srcSchema     The schema of the source DataFrame.
      * @param bigtableSchema The schema of the Bigtable DataFrame.
      * @param joinColumns   The columns used for joining, which should be excluded from Bigtable schema.
      * @return A StructType representing the merged schema.
      */
    private def getCombinedSchema(
        srcSchema: StructType,
        bigtableSchema: StructType,
        joinColumns: Array[String]
    ): StructType = {
      val bigtableSchemaNew = bigtableSchema.fields
        .filterNot(field => joinColumns.contains(field.name))
      val combinedFields = (srcSchema.fields ++ bigtableSchemaNew).distinct
      StructType(combinedFields)
    }

    /** Fetches rows from a Bigtable table based on the provided row keys.
      *
      * @param rowKeys        Array of row keys to be fetched.
      * @param clientKey      The Bigtable client configuration key.
      * @param tableId        The ID of the Bigtable table to query.
      * @param pNumber        The partition number (for logging purposes).
      * @param rowKeyBatchSize The size of each batch of row keys for querying Bigtable.
      * @return An iterator of BigtableRow objects containing the retrieved rows.
      */
    private def fetchBigtableRows(
        rowKeys: Array[String],
        clientKey: BigtableClientKey,
        tableId: String,
        pNumber: Int,
        rowKeyBatchSize: Int
    ): Iterator[BigtableRow] = {
      try {
        val rowKeyBatches = rowKeys.grouped(rowKeyBatchSize)
        rowKeyBatches.flatMap { batch =>
          val clientHandle = BigtableDataClientBuilder.getHandle(clientKey)
          val bigtableClient = clientHandle.getClient()
          val query = Query.create(TableId.of(tableId))
          batch.foreach { rowKey =>
            val rowKeyBytes = ByteString.copyFrom(BytesConverter.toBytes(rowKey))
            query.rowKey(rowKeyBytes)
          }
          val responseStream = bigtableClient.readRows(query)
          convertStreamToIterator(responseStream, clientHandle)
        }
      } catch {
        case e: Exception =>
          val errorMsg =
            s"Failed to fetch Bigtable rows for table '$tableId' in partition $pNumber. " +
              s"RowKeyBatchSize: $rowKeyBatchSize, Total RowKeys: ${rowKeys.length}. " +
              s"Error: ${e.getMessage}"
          logError(errorMsg, e)
          throw new RuntimeException(errorMsg, e)
      }
    }

    /** Converts a Bigtable ServerStream into an Iterator. */
    private def convertStreamToIterator(
        stream: ServerStream[BigtableRow],
        clientHandle: BigtableDataClientBuilder.DataClientHandle
    ): Iterator[BigtableRow] = {
      val rowStreamIterator = stream.iterator()
      new Iterator[BigtableRow] {
        override def hasNext: Boolean = {
          if (!rowStreamIterator.hasNext) {
            stream.cancel()
            clientHandle.close()
            false
          } else true
        }
        override def next(): BigtableRow = rowStreamIterator.next()
      }
    }
  }
}

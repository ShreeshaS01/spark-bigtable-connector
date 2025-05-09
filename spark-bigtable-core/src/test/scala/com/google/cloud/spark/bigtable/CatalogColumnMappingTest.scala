package com.google.cloud.spark.bigtable

import com.google.bigtable.v2.SampleRowKeysResponse
import com.google.cloud.spark.bigtable.datasources.BytesConverter
import com.google.cloud.spark.bigtable.fakeserver.{FakeCustomDataService, FakeServerBuilder}
import com.google.protobuf.ByteString
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class CatalogColumnMappingTest
  extends AnyFunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with Logging {

  @transient lazy implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("CatalogColumnMappingTest")
    .master("local[*]")
    .getOrCreate()

  var fakeCustomDataService: FakeCustomDataService = _
  var emulatorPort: String = _

  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  override def beforeEach(): Unit = {
    fakeCustomDataService = new FakeCustomDataService
    val server = new FakeServerBuilder().addService(fakeCustomDataService).start
    emulatorPort = Integer.toString(server.getPort)
    logInfo("Bigtable mock server started on port " + emulatorPort)
  }

  test("rowkey columns are mapped") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "expected-value",
      "cf1",
      "doesnt matter",
      "some value"
    )

    addSampleKeyResponse("expected-value")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[String]("key") == "expected-value")
  }

  test("non row key columns are mapped") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"},
         |"someCol":{"cf":"cf1", "col":"btcol", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "btcol",
      "expected-value"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[String]("someCol") == "expected-value")
  }

  test("a single regex match maps correctly") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "col":".*", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "any",
      "expected-value"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[Map[String, String]]("someCol").get("any") == Some("expected-value"))
  }

  test("a static column that also maps a regex will be mapped to both columns") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"},
         |"staticCol":{"cf":"cf1", "col":"any", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "col":".*", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "any",
      "expected-value"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[Map[String, String]]("someCol").get("any") == Some("expected-value"))
    assert(result.first().getAs[String]("staticCol") == "expected-value")
  }

  test("multiple columns mapping the same regex will be added to the regex") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "col":".*", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "another",
      "another-expected-value"
    )

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "any",
      "expected-value"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[Map[String, String]]("someCol").get("any") == Some("expected-value"))
    assert(result.first().getAs[Map[String, String]]("someCol").get("another") == Some("another-expected-value"))
  }

  test("single column mapping to multiple regexes will be added to all") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "col":".*", "type":"string"},
         |"anotherCol":{"cf":"cf1", "col":".*", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "any",
      "expected-value"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[Map[String, String]]("someCol").get("any") == Some("expected-value"))
    assert(result.first().getAs[Map[String, String]]("anotherCol").get("any") == Some("expected-value"))
  }

  test("non matching column will not be added to regex column") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "col":"^a.*", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "any",
      "expected-value"
    )

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "not-this-one",
      "whatever"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[Map[String, String]]("someCol").get("any") == Some("expected-value"))
    assert(!result.first().getAs[Map[String, String]]("someCol").contains("not-this-one"))
  }

  test("matching column qualifier but in different column family will not be added to regex column") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"}
         |},
         |"regexColumns":{
         |"someCol":{"cf":"cf1", "col":".*", "type":"string"}
         |}
         |}""".stripMargin

    fakeCustomDataService.addRow(
      "some-row",
      "cf1",
      "any",
      "expected-value"
    )

    fakeCustomDataService.addRow(
      "some-row",
      "cf2",
      "any2",
      "whatever"
    )

    addSampleKeyResponse("some-row")

    val result = spark
      .read
      .format("bigtable")
      .options(createParametersMap(catalog))
      .load()

    assert(result.first().getAs[Map[String, String]]("someCol").get("any") == Some("expected-value"))
    assert(!result.first().getAs[Map[String, String]]("someCol").contains("any2"))
  }

  test("catalog with duplicated column name will throw an error") {
    val catalog =
      s"""{
         |"table":{"name":"tableName"},
         |"rowkey":"row-key",
         |"columns":{
         |"key":{"cf":"rowkey", "col":"row-key", "type":"string"},
         |"repeated-col":{"cf":"cf1", "col":"any", "type":"string"}
         |},
         |"regexColumns":{
         |"repeated-col":{"cf":"cf2", "col":".*", "type":"string"}
         |}
         |}""".stripMargin

    assertThrows[AnalysisException] {
      spark
        .read
        .format("bigtable")
        .options(createParametersMap(catalog))
        .load()
    }
  }

  def createParametersMap(catalog: String): Map[String, String] = {
    Map(
      "catalog" -> catalog,
      "spark.bigtable.project.id" -> "fake-project-id",
      "spark.bigtable.instance.id" -> "fake-instance-id",
      "spark.bigtable.emulator.port" -> emulatorPort
    )
  }

  def addSampleKeyResponse(row: String): Unit = {
    fakeCustomDataService.addSampleRowKeyResponse(
      SampleRowKeysResponse
        .newBuilder()
        .setRowKey(ByteString.copyFrom(BytesConverter.toBytes(row)))
        .build())
  }
}

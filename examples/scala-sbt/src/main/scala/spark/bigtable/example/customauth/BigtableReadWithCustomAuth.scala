package spark.bigtable.example.customauth

import org.apache.spark.sql.SparkSession
import spark.bigtable.example.WordCount.parse

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object BigtableReadWithCustomAuth extends App {

  val (projectId, instanceId, tableName, _) = parse(args)

  val spark = SparkSession.builder()
    .appName("BigtableReadWithCustomAuth")
    .master("local[*]")
    .config("spark.executor.memory", "12g")
    .config("spark.executor.cores", "16")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.bigtable.grpc.retry.enabled", "true")
    .config("spark.bigtable.grpc.retry.maxAttempts", "5")
    .config("spark.bigtable.grpc.retry.initialBackoff.ms", "1000")
    .config("spark.bigtable.grpc.retry.maxBackoff.ms", "10000")
    .getOrCreate()

  spark.conf.set("spark.sql.adaptive.enabled", "false")

  // Initialize custom AccessTokenProvider
  val tokenProvider = new CustomAccessTokenProvider()
  val initialToken = tokenProvider.getCurrentToken
  println(s"initial token: $initialToken")

  // Bigtable catalog configuration
  val catalog: String =
    s"""{
       |"table":{"namespace":"default", "name":"$tableName", "tableCoder":"PrimitiveType"},
       |"rowkey":"order_id",
       |"columns":{
       |  "order_id":{"cf":"rowkey", "col":"order_id", "type":"string"},
       |  "customer_id":{"cf":"order_info", "col":"customer_id", "type":"string"},
       |  "product_id":{"cf":"order_info", "col":"product_id", "type":"string"},
       |  "amount":{"cf":"order_info", "col":"amount", "type":"string"},
       |  "order_date":{"cf":"order_info", "col":"order_date", "type":"string"},
       |  "status":{"cf":"order_info", "col":"status", "type":"string"},
       |  "metadata":{"cf":"order_info", "col":"metadata", "type":"string"},
       |  "logs":{"cf":"order_info", "col":"logs", "type":"string"},
       |  "comments":{"cf":"order_info", "col":"comments", "type":"string"}
       |}
       |}""".stripMargin

  Future {
    Thread.sleep(50000) // Give main thread 80s
    tokenProvider.refresh()
  }.map { _ =>
    val refreshedToken = tokenProvider.getCurrentToken
    println(s"Refreshed token: $refreshedToken")

    // Verify that the token has changed
    if (initialToken != refreshedToken) {
      println("Token refresh test passed: The token has changed.")
    } else {
      println("Token refresh test failed: The token has not changed.")
    }
  }

  try {
    val readDf = spark.read
      .format("bigtable")
      .option("catalog", catalog)
      .option("spark.bigtable.project.id", projectId)
      .option("spark.bigtable.instance.id", instanceId)
      .option("spark.bigtable.gcp.accesstoken.provider", tokenProvider.getClass.getName)
      .load()

    // mkdir a data folder before running this
    println("Reading data from Bigtable...")
    val savePath = "~/Documents/data"
    readDf
      .show(50)
//      .sample(0.0000005)
//      .repartition(10)
//      .write
//      .mode("overwrite")
//      .format("parquet")
//      .save(savePath)
  } catch {
    case e: Exception =>
      println(s"Error reading/writing data: ${e.getMessage}")
      e.printStackTrace()
  } finally {
    spark.stop()
  }
}

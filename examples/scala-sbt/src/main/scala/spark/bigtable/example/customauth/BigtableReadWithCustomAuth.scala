package spark.bigtable.example.customauth

import org.apache.spark.sql.SparkSession
import spark.bigtable.example.WordCount.parse

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object BigtableReadWithCustomAuth extends App {

  val (projectId, instanceId, tableName, _) = parse(args)

  System.setProperty("hadoop.home.dir", "C:\\Hadoop")

  val spark = SparkSession.builder()
    .appName("BigtableReadWithCustomAuth")
    .master("local[*]")
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

  // Read data from Bigtable
  val readDf = spark.read
    .format("bigtable")
    .option("catalog", catalog)
    .option("spark.bigtable.project.id", projectId)
    .option("spark.bigtable.instance.id", instanceId)
    .option("spark.bigtable.custom.token.provider", tokenProvider.getClass.getName)
    .load()

  Future {
    Thread.sleep(80000) // Give main thread 80s
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

  // Simulate delay to trigger token refresh
  println("Reading data from Bigtable...")
  val savePath = "C:\\Users\\cloudsufi\\Documents\\custom-auth\\spark-bigtable-connector\\examples\\scala-sbt\\data"
  readDf
    //    .show(10000)
    .sample(0.0000005)
    .coalesce(1)
    .write
    .format("csv")
    .mode("overwrite")
    .save(savePath)

  spark.stop()
}

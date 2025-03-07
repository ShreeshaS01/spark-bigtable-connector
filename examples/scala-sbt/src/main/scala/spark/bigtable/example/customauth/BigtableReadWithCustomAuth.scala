package spark.bigtable.example.customauth

import com.google.cloud.spark.bigtable.Logging
import org.apache.spark.sql.SparkSession
import spark.bigtable.example.WordCount.parse

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object BigtableReadWithCustomAuth extends App with Logging {

  val (projectId, instanceId, tableName, _) = parse(args)

  val spark = SparkSession.builder()
    .appName("BigtableReadWithCustomAuth")
    .master("local[*]")
    .getOrCreate()

  val tokenProvider = new CustomAccessTokenProvider()
  val initialToken = tokenProvider.getCurrentToken

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

  tokenRefreshTest()

  try {
    val readDf = spark.read
      .format("bigtable")
      .option("catalog", catalog)
      .option("spark.bigtable.project.id", projectId)
      .option("spark.bigtable.instance.id", instanceId)
//      .option("spark.bigtable.gcp.accesstoken.provider", tokenProvider.getClass.getName)
      .load()

    logInfo("Reading data from Bigtable...")
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
      logInfo(s"Error reading/writing data: ${e.getMessage}")
      e.printStackTrace()
  } finally {
    spark.stop()
  }

  private def tokenRefreshTest() = {
    Future {
      Thread.sleep(20000) // Give main thread 20s
      tokenProvider.refresh()
    }.map { _ =>
      val refreshedToken = tokenProvider.getCurrentToken

      // Verify that the token has changed
      if (initialToken != refreshedToken) {
        logInfo("Token refresh test passed: The token has changed.")
      } else {
        logInfo("Token refresh test failed: The token has not changed.")
      }
    }
  }
}

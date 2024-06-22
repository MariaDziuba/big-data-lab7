import org.apache.spark.sql.{DataFrame, SparkSession}
import preprocess.{Preprocessor}
import db.{MySqlDatabase}
import com.typesafe.scalalogging.Logger


class DataMart(HOST: String) {
  private val USER = "root"
  private val PASSWORD = "password"
  private val APP_NAME = "KMeans"
  private val DEPLOY_MODE = "local"
  private val DRIVER_MEMORY = "2g"
  private val EXECUTOR_MEMORY = "2g"
  private val EXECUTOR_CORES = 1
  private val DRIVER_CORES = 1
  private val MYSQL_CONNECTOR_JAR = "../jars/mysql-connector-j-8.4.0.jar"
  private val PARQUET_PATH = "/shared/parquet_openfoodfacts.parquet"
  private val DYNAMIC_ALLOCATION = true
  private val MIN_EXECUTORS = 1
  private val MAX_EXECUTORS = 10
  private val INITIAL_EXECUTORS = 2

  val session = SparkSession.builder
    .appName(APP_NAME)
    .master(DEPLOY_MODE)
    .config("spark.driver.host", HOST)
    .config("spark.driver.bindAddress", HOST)
    .config("spark.driver.cores", DRIVER_CORES)
    .config("spark.executor.cores", EXECUTOR_CORES)
    .config("spark.driver.memory", DRIVER_MEMORY)
    .config("spark.executor.memory", EXECUTOR_MEMORY)
    .config("spark.dynamicAllocation.enabled", DYNAMIC_ALLOCATION)
    .config("spark.dynamicAllocation.minExecutors", MIN_EXECUTORS)
    .config("spark.dynamicAllocation.maxExecutors", MAX_EXECUTORS)
    .config("spark.dynamicAllocation.initialExecutors", INITIAL_EXECUTORS)
    .config("spark.jars", MYSQL_CONNECTOR_JAR)
    .config("spark.driver.extraClassPath", MYSQL_CONNECTOR_JAR)
    .getOrCreate()
  private val db = new MySqlDatabase(session, HOST)
  private val logger = Logger("Logger")

  def main(args: Array[String]): Unit = {
    val transformed = readPreprocessedOpenFoodFactsDataset()
    transformed.write.mode(SaveMode.Overwrite).parquet(PARQUET_PATH)
    while (true) {
      Thread.sleep(1000)
    }
  }

  def readPreprocessedOpenFoodFactsDataset(): DataFrame = {
    val data = db.readTable("OpenFoodFacts")
    logger.info("The OpenFoodFacts table was successfully read")

    val transforms: Seq[DataFrame => DataFrame] = Seq(
      Preprocessor.fillNa,
      Preprocessor.assembleVector,
      Preprocessor.scaleAssembledDataset
    )

    val transformed = transforms.foldLeft(data) { (df, f) => f(df) }
    logger.info("All transforms were applied to the dataset")
    transformed
  }

  def writePredictions(df: DataFrame): Unit = {
    db.insertDf(df, "Predictions")
    logger.info("All predictions were inserted in the Predictions table")
  }
}
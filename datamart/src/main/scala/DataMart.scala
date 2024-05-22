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
  private val MYSQL_CONNECTOR_JAR = "../mysql-connector-j-8.4.0.jar"
  val session = SparkSession.builder
    .appName(APP_NAME)
    .master(DEPLOY_MODE)
    .config("spark.driver.host", HOST)
    .config("spark.driver.bindAddress", HOST)
    .config("spark.driver.cores", DRIVER_CORES)
    .config("spark.executor.cores", EXECUTOR_CORES)
    .config("spark.driver.memory", DRIVER_MEMORY)
    .config("spark.executor.memory", EXECUTOR_MEMORY)
    .config("spark.jars", MYSQL_CONNECTOR_JAR)
    .config("spark.driver.extraClassPath", MYSQL_CONNECTOR_JAR)
    .getOrCreate()
  private val db = new MySqlDatabase(session, HOST)
  private val logger = Logger("Logger")


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
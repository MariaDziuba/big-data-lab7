package db

import org.apache.spark.sql.{DataFrame, SparkSession}


class MySqlDatabase(spark: SparkSession, HOST: String) {
  private val PORT = 3306
  private val DATABASE = "lab6"
  private val JDBC_URL = s"jdbc:mysql://$HOST:$PORT/$DATABASE"
  private val USER = "root"
  private val PASSWORD = "password"

  def readTable(tablename: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", JDBC_URL)
      .option("user", USER)
      .option("password", PASSWORD)
      .option("dbtable", tablename)
      .option("inferSchema", "true")
      .load()
  }

  def insertDf(df: DataFrame, tablename: String): Unit = {
    df.write
      .format("jdbc")
      .option("url", JDBC_URL)
      .option("user", USER)
      .option("password", PASSWORD)
      .option("dbtable", tablename)
      .mode("append")
      .save()   
  }
}
from pyspark.sql import DataFrame, SparkSession, SQLContext
import time

class DataMart:
    def __init__(self, spark: SparkSession, host: str):
        self.spark_context = spark.sparkContext
        self.sql_context = SQLContext(self.spark_context, spark)
        self.jwm_datamart = self.spark_context._jvm.DataMart(host)

    def read_dataset(self) -> DataFrame:
        jvm_data = self.jwm_datamart.readPreprocessedOpenFoodFactsDataset()
        result = DataFrame(jvm_data, self.sql_context)
        timestr = time.strftime("%Y%m%d-%H%M%S")
        result.write.csv(f"./csvs/{timestr}.csv")
        return result

    def write_predictions(self, df: DataFrame):
        self.jwm_datamart.writePredictions(df._jdf)
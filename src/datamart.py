from pyspark.sql import DataFrame, SparkSession, SQLContext


class DataMart:
    def __init__(self, spark: SparkSession, host: str):
        self.spark_context = spark.sparkContext
        self.sql_context = SQLContext(self.spark_context, spark)
        self.jwm_datamart = self.spark_context._jvm.DataMart(host)

    def read_dataset(self) -> DataFrame:
        jvm_data = self.jwm_datamart.readPreprocessedOpenFoodFactsDataset()
        return DataFrame(jvm_data, self.sql_context)

    def write_predictions(self, df: DataFrame):
        self.jwm_datamart.writePredictions(df._jdf)
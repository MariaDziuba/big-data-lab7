import pathlib
import configparser
import os
import path
import sys
cur_dir = path.Path(__file__).absolute()
sys.path.append(cur_dir.parent.parent)
tmp_dir = os.path.join(cur_dir.parent.parent.parent, "tmp")
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession
from create_tables import create_open_food_facts
import loguru
import time


class KMeansClustering:

    def clustering(self, scaled_data):
        loguru.logger.info("Clustering started")
        evaluator = ClusteringEvaluator(
            predictionCol='prediction',
            featuresCol='scaled_features',
            metricName='silhouette',
            distanceMeasure='squaredEuclidean'
        )

        for k in range(2, 10):
            kmeans = KMeans(featuresCol='scaled_features', k=k)
            model = kmeans.fit(scaled_data)
            predictions = model.transform(scaled_data)
            score = evaluator.evaluate(predictions)
            loguru.logger.info(f'k = {k}, silhouette score = {score}')

        loguru.logger.info("Clustering finished")


def main():

    create_open_food_facts()

    config = configparser.ConfigParser()
    config.read('config.ini')

    # .config("spark.driver.host", "127.0.0.1") \
    # .config("spark.driver.bindAddress", "127.0.0.1") \
    
    sql_connector_path = os.path.join(cur_dir.parent.parent, config['spark']['mysql_connector_jar'])
    scala_logging_path = os.path.join(cur_dir.parent.parent, config['spark']['scala_logging'])
    parquet_path = os.path.join(cur_dir.parent.parent, config['data']['parquet_path'])

    spark = SparkSession.builder \
    .appName(config['spark']['app_name']) \
    .master(config['spark']['deploy_mode']) \
    .config("spark.driver.cores", config['spark']['driver_cores']) \
    .config("spark.executor.cores", config['spark']['executor_cores']) \
    .config("spark.driver.memory", config['spark']['driver_memory']) \
    .config("spark.executor.memory", config['spark']['executor_memory']) \
    .config("spark.dynamicAllocation.enabled", config['spark']['dynamic_allocation']) \
    .config("spark.dynamicAllocation.minExecutors", config['spark']['min_executors']) \
    .config("spark.dynamicAllocation.maxExecutors", config['spark']['max_executors']) \
    .config("spark.dynamicAllocation.initialExecutors", config['spark']['initial_executors']) \
    .config("spark.jars", f"{sql_connector_path},jars/datamart.jar,{scala_logging_path}") \
    .config("spark.driver.extraClassPath", sql_connector_path) \
    .getOrCreate()

    loguru.logger.info("Created a SparkSession object")

    while(True):
        if os.path.isdir(parquet_path):
            break
        time.sleep(10)

    assembled_data = spark.read.parquet(parquet_path)
    kmeans = KMeansClustering()
    kmeans.clustering(assembled_data)

    spark.stop()


if __name__ == '__main__':
    main()
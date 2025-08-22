import os
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException


class read_api:
    def __init__(self, api_url: str, folder_path_silver: str):
        """
        api_url: URL endpoint that returns JSON (a list of dicts)
        folder_path_silver: Target location for silver data (e.g., DBFS or local path)
        """
        self.api_url = api_url
        self.folder_path_silver = folder_path_silver
        self.spark = SparkSession.builder.appName("TIP").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

        self.username = os.environ.get("tip_challenge_username")
        self.password = os.environ.get("tip_challenge_password")

        if not self.username or not self.password:
            raise ValueError("Missing environment variables: tip_challenge_username or tip_challenge_password")

    def fetch_api_data(self):
        try:
            response = requests.get(self.api_url, auth=(self.username, self.password))
            response.raise_for_status()
            data = response.json()
            df = self.spark.createDataFrame(data)

            #add surrogate_key, this can also be imported from the config, and then used as part of the metadata ingestion framework
            df = df.withColumn("surrogate_key", col("WSM_key").cast("string"))
            return df 
        except Exception as e:
            print(f"Error fetching API data: {e}")
            return []


    def incremental_load(self):
        self.spark.catalog.clearCache()  # Clear metadata cache

        new_data = self.fetch_api_data()

        # Temporary staging directory to avoid race condition
        temp_path = self.folder_path_silver + "/_tmp"

        new_data.createOrReplaceTempView("new_data")
        new_data.write.mode("overwrite").csv(temp_path, header=True)

        try:
            source_data = self.spark.read.csv(self.folder_path_silver + "/*.csv", header=True)
            source_data.createOrReplaceTempView("source_data")
        except AnalysisException as e:
            print(f"Error reading source data: {e}")
            source_data = self.spark.createDataFrame([], new_data.schema)
            source_data.createOrReplaceTempView("source_data")

        merged_data = self.spark.sql("""
            SELECT new_data.*
            FROM new_data
            LEFT ANTI JOIN source_data
            ON new_data.surrogate_key = source_data.surrogate_key
            UNION
            SELECT * FROM source_data
        """).dropDuplicates(["surrogate_key"])

        try:
            merged_data.write.mode("overwrite").csv(self.folder_path_silver, header=True)
            print("Data written successfully")
        except Exception as e:
            print(f"Error writing data: {e}")
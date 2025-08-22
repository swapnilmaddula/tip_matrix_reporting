import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

class LoadTweetData:
    def __init__(self, file_path_source, folder_path_silver):
        self.new_data = []
        self.file_path_source = file_path_source 
        self.folder_path_silver = folder_path_silver 
        self.spark = SparkSession.builder.appName("Elsevier").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def read_source_file(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            data = file.readlines()
        json_lines = data
        rows = []
        for line in json_lines:
            try:
                tweet = json.loads(line.strip())
                tweet_id = tweet['twitter']['id']
                tweet_id = int(tweet_id)
                created_at_raw = tweet['interaction']['created_at']
                created_at = datetime.strptime(created_at_raw, '%a, %d %b %Y %H:%M:%S +0000').isoformat()
                content = tweet['interaction']['content']
                rows.append([created_at, tweet_id, content])
            except Exception as e:
                pass
        return rows

    #incremental load
    def incremental_load(self):
        rows = self.read_source_file(self.file_path_source)
        schema = StructType([
            StructField("created_at", StringType(), True),
            StructField("tweet_id", LongType(), True),
            StructField("content", StringType(), True)
        ])
        
        new_data = self.spark.createDataFrame(rows, schema)
        new_data.createOrReplaceTempView("new_data")
        new_data.write.mode("overwrite").options(delimiter="#@#@").csv(path=self.folder_path_silver, header=True)


        try:
            source_data = self.spark.read.options(delimiter="#@#@").csv(path=f"{self.folder_path_silver}/*.csv", header=True, schema=schema)
            source_data.createOrReplaceTempView("source_data")
        except Exception as e:
            print(f"Error reading source data: {e}")
            source_data = self.spark.createDataFrame([], schema)
            source_data.createOrReplaceTempView("source_data")


        # SQL query to merge new data into existing data
        merged_data = self.spark.sql("""
            SELECT new_data.created_at, new_data.tweet_id, new_data.content
            FROM new_data
            LEFT ANTI JOIN source_data
            ON new_data.tweet_id = source_data.tweet_id
            UNION
            SELECT * FROM source_data
        """)
        merged_data.dropDuplicates()
        try:
            merged_data.write.mode("overwrite").options(delimiter="#@#@").csv(path=self.folder_path_silver, header=True)
            print("Data written successfully")
        except Exception as e:
            print(f"Error writing data: {e}")
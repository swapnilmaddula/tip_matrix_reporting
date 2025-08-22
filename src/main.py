import identify_trending_topics
import read_source_data
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Elsevier").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def main(source_file_path, silver_path, gold_path):

    load_tweet_data = read_source_data.LoadTweetData(file_path_source=source_file_path, folder_path_silver=silver_path)
    load_tweet_data.incremental_load()

    top5trends = identify_trending_topics.Top5Trends(filepath_silver=f"{silver_path}/*.csv", folderpath_gold=gold_path)
    top5trends.identify_trending_topics()

if __name__ == "__main__":

    main(source_file_path='data/source_data/dataset1.json', silver_path='data/silver/tweet_data', gold_path='data/gold/top5trends')
    
spark.stop()
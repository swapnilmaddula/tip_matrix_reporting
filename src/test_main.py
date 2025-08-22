import pytest
import identify_trending_topics
import read_source_data
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Elsevier").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. test for spam data - Validation
    # test setup: silver table defined only with spam content
    # expectation: no trending topics should be written to the gold layer

def test_main1():

    silver_path = "test_data/silver"
    gold_path = "test_data/gold/top5trends"

    top5trends = identify_trending_topics.Top5Trends(filepath_silver=f"{silver_path}/*.csv", folderpath_gold=gold_path)
    top5trends.identify_trending_topics()

    gold_table = spark.read.csv(path = gold_path+ "/*csv", header = True)

    gold_table.show

    gold_table.createOrReplaceTempView('data')

    row_count = spark.sql('SELECT COUNT(*) FROM data WHERE LENGTH(TRENDING_TOPICS)> 0').collect()[0][0]

    print(row_count)
    
    assert(row_count) == 0

# 2. test for duplicates - regression
    # test setup: logic implemented for deduplication
    # expectation: silver table should not contain duplicates


def test_main2():

    source_path = "test_data/source_data/dataset1.json"
    silver_path = "test_data/silver/tweet_data"

    Read_source = read_source_data.LoadTweetData(file_path_source=source_path, folder_path_silver= silver_path)
    Read_source.incremental_load()

    silver_table = spark.read.csv(path = silver_path+ "/*csv", header = True)

    duplicate_rows_count = silver_table.count() - silver_table.distinct().count()
    print("this is count --------------------------")
    print(duplicate_rows_count)
    assert(duplicate_rows_count) == 0

if __name__ == "__main__":
    pytest.main([__file__])
    








    



    


    

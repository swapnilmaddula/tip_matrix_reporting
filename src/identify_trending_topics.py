from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, collect_list, concat_ws, udf, StringType
import nltk
from collections import Counter
import re

nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')

#Identify top 5 trends
class Top5Trends:
    
    def __init__(self, filepath_silver, folderpath_gold):
        self.filepath_silver = filepath_silver
        self.folderpath_gold = folderpath_gold
        self.tweets = None
        self.spark = SparkSession.builder.appName("Elsevier").getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")


    def read_csv(self):
        tweet_data = self.spark.read.options(delimiter="#@#@").csv(self.filepath_silver, header=True, inferSchema=True, multiLine=True)
        tweet_data.show()
        return tweet_data
    
    def obtain_list_of_ignore_words(self, language):
        stop_words = nltk.corpus.stopwords.words(language)
        return stop_words
    
    def identify_trending_topics(self):
        tweet_data = self.read_csv()
        grouped_tweets = tweet_data.withColumn("date", F.to_date("created_at"))
        grouped_tweets = grouped_tweets.groupBy("date").agg(F.concat_ws(" ", F.collect_list("content")).alias("concatenated_content"))
        grouped_tweets = grouped_tweets.na.drop(subset = ["date"])
        stop_words = self.obtain_list_of_ignore_words("dutch")
        unwanted_characters_regex= re.compile(r'[^a-zA-Z\s]')

        #define logic for selecting and filtering words
        def extract_trending_topics(content):
            content = unwanted_characters_regex.sub('', content)
            tokens = nltk.word_tokenize(content)
            filtered_tokens = [w for w in tokens if not w.lower() in stop_words and len(w) >= 3]
            word_counter = Counter(filtered_tokens)
            most_common_words = word_counter.most_common(5)
            most_common_words_string = ','.join([word for word, count in most_common_words])
            return most_common_words_string
        
        #create udf 
        extract_trending_topics_udf = F.udf(extract_trending_topics, StringType())

        #concatenate top5 for each date
        grouped_tweets = grouped_tweets.withColumn('trending_topics', extract_trending_topics_udf(F.col('concatenated_content')))
        grouped_tweets = grouped_tweets["date","trending_topics"]

        #write to gold
        grouped_tweets.write.csv(path = self.folderpath_gold,header = True, mode = "overwrite")
        grouped_tweets.show()
    
        



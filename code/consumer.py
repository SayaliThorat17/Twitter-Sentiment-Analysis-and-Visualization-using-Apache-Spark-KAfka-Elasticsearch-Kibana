from elasticsearch import helpers
from elasticsearch import Elasticsearch
import nltk
nltk.download('vader_lexicon')
# import sys
from ownelastic import sth2elastic
from pyspark.sql import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import timezone
from datetime import datetime
from textblob import TextBlob



def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


def dosentiment(tweet):
    #dict_data = json.loads(tweet)
    t = TextBlob(tweet)
    #t = TextBlob(tweet["text"])
    print (t.sentiment.polarity)
    if t.sentiment.polarity < 0:
        sentiment = "negative"
    elif t.sentiment.polarity == 0:
        sentiment = "neutral"
    else:
        sentiment = "positive"
    print(sentiment)
    return sentiment


def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        if rdd.count()==0: raise Exception('Empty')
        sqlContext = getSqlContextInstance(rdd.context)
        df = sqlContext.read.json(rdd)
        df = df.filter("text not like 'RT @%'")
        if df.count() == 0: raise Exception('Empty')
        udf_func = udf(lambda x: dosentiment(x),returnType=StringType())
        df = df.withColumn("sentiment",lit(udf_func(df.text)))
        print(df.take(10))
        results = df.toJSON().map(lambda j: json.loads(j)).collect()
        for result in results:
            result["date"]= datetime.strptime(result["date"],"%Y-%m-%d %H:%M:%S")
           # result["sentiment"]=json.loads(result["sentiment"])
        sth2elastic(results,"twit","doc")
        es.index
    except Exception as e:
        print(e)
        pass

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreaming")
    ssc = StreamingContext(sc, 3)
    kafkaStream = KafkaUtils.createStream(ssc, "your local host", "console-consumer", {"twit": 1})
    lines = kafkaStream.map(lambda x: json.loads(x[1]))

    lines.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
    
    
    
    
    
# Twitter-Sentiment-Analysis-and-Visualization-using-Apache-Spark-KAfka-Elasticsearch-Kibana




Sentiment Analysis for twitter is nothing but classifying the opinions expressed in tweets.


-> Installed :

Apache Kafka, 
Apache Spark, 
Elasticsearch, 
Kibana - Dashboard for Visualization



-> Architecture :

Around 6000 tweets per second are tweeted on Twitter. Twitter offers us an API where we can access these tweets. For this we just need to get our own API tokens.

Written in python, a Kafka Producer is used for reading the tweet's livestream. It does the cleaning and send the required part or cleaned data to the topic created on Server.
On the other side of server, Kafka consumer is waiting for the message queue. Once it collects the message, it does the process of sentiment analysis.

For sentiment analysis, TextBlob (a python library) is used. It does the calculation and gives us polarity i.e. a value between -1 to 1. -1 to 0 means negative sentiment 0 is neutral 0 to +1 is positive sentiment
After sentiment analysis we represent the data visually on a Kibana dashboard with the help of Elasticsearch.

-> To present some visualization, I filtered the tweets on hashtags of "#Elections2020".






from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json
from datetime import datetime
from datetime import timezone
import json
from textblob import TextBlob
from elasticsearch import Elasticsearch


access_token = "your key"
access_token_secret = "your key"
consumer_key = "your key"
consumer_secret = "your key"


def cleantweet(data):
    rawtweet = json.loads(data)
    tweet={}
    tweet["date"] = datetime.strptime(rawtweet["created_at"],'%a %b %d %H:%M:%S %z %Y')\
        .replace(tzinfo=timezone.utc).astimezone(tz=None).strftime('%Y-%m-%d %H:%M:%S')
    tweet["user"] = rawtweet["user"]["screen_name"]
    tweet["language"]="en"
    tweet["location"]=rawtweet["user"]["location"]
    tweet["retweet"]=rawtweet["retweet_count"]
   
    return json.dumps(tweet)



class StdOutListener(StreamListener):
    def on_data(self, data):
        newdata = cleantweet(data)
        producer.send("twit", newdata)
        print(newdata)
        return True
    def on_error(self, status):
        print (status)
        

producer = KafkaProducer(bootstrap_servers='your localhost', api_version=(0, 10, 1),value_serializer=lambda m: json.dumps(m).encode('ascii'))
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l,tweet_mode='extended')
stream.filter(track=["#trump","#elections2020"], languages=["en"])










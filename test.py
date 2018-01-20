from __future__ import print_function
import tweepy
import json
from pymongo import MongoClient
import sys
import emoji
import folium
import copy
from PIL import Image
from PIL import ImageDraw


from PIL import ImageFont
import png



from textblob import TextBlob


MONGO_HOST = 'mongodb://localhost/twitterdb'  # assuming you have mongoDB installed locally
# and a database called 'twitterdb'

WORDS = ['#bigdata', '#AI', '#datascience', '#machinelearning', '#ml', '#deeplearning']
sta={'AL','AK','AR','AZ' ,'CA','CO','CT','DE','FL','HI','IA','ID','IL','IN','KS','KY','LA','MA','ME','MD','MI','MN','MO'
,'MS','MT','NE','NC','ND','NH','NJ','NM','NV','NY','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','DC','WI','WY'}
#keys_file = open("keys.txt")
#lines = keys_file.readlines()
consumer_key = "DTbGErVRPgyr33w0I6SxHzLj3"
consumer_secret = "6B1d8oNvEKaZXD5WwE9LrKP7UvxJgOARBk8WDS99NaHH3wwNl1"
access_token = "937853958713995264-rrcBeBbIyg7N0LlkVAwoq5NHwkd2HBY"
access_token_secret = "SuYShQTzGyDfRieOdVwSfqJqalFu4WheiRxFnZmLy8Sa8"
num=0;


#    thedict.update({key_a:{key_b: val}})
def addwordindict(dict,key1,key2):
    if key1 not in dict.keys():
        dict.update({key1:{key2:1}})
    else:
        if key2 in dict[key1].keys():
            dict[key1].update({key2:dict[key1][key2]+1})
        else:
            dict[key1].update({key2:1})
# Define a function to sort the dictionary according to their values
def sortdic(dict):
    items=dict.items()
    backitems = [[v[1], v[0]] for v in items]
    backitems.sort(reverse=True)
    return backitems



class StreamListener(tweepy.StreamListener):
    # This is a class provided by tweepy to access the Twitter Streaming API.

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False

    def on_data(self, data):
        # This is the meat of the script...it connects to your mongoDB and stores the tweet
        global num
        try:
            client = MongoClient(MONGO_HOST)

            # Use twitterdb database. If it doesn't exist, it will be created.
            db = client.usa_db

            # Decode the JSON from Twitter
            datajson = json.loads(data)

            # grab the 'created_at' data from the Tweet to use for display
            created_at = datajson['created_at']


            # print out a message to the screen that we have collected a tweet


            # insert the data into the mongoDB into a collection called twitter_search
            # if twitter_search doesn't exist, it will be created.
            if datajson['coordinates']!=None:
              db.usa_tweets_collection.insert(datajson)
              num=num+1
              print("Tweet collected at " + str(created_at))
            if(num==10000):
                sys.exit()
        except Exception as e:
            print(e)


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
# Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.

listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True))
streamer = tweepy.Stream(auth=auth, listener=listener)
#Store 10000 tweets in mongodb
#PART A---------------------------------------------------------------------------------

#streamer.filter(locations=[-124, 25, -66, 48])
client = MongoClient('localhost', 27017)
db = client['usa_db']
collection = db['usa_tweets_collection']
tweets_iterator = collection.find()

#-----------------------------------------------------------------------------
# Store emoji and state in dictionary state emoj allemoj

state={}
emoj={}
allemoj={}
state_count={}
ca={}
map = folium.Map(location=[48, -102], zoom_start=13)
for tweet in tweets_iterator:
    #addmap
    '''folium.CircleMarker(tweet['geo']['coordinates'],
                        radius=0.001,
                        popup='tweet site',
                        color='#3186cc',
                        fill_color='#3186cc',).add_to(map)'''

    if tweet['place']!=None:
       location=tweet['place']['full_name']
    if location!=None:
       #print(location)
       loc=location.split(',')
       #print(loc[0])
        #dictionary state key: MA[emoji1] ,NY, CA
    #get the location of the tweet
    flag=0

    for i in loc:
        #print(i)
        if i.strip() in sta:
            flag=i.strip()
            break
    if tweet['text']==None:
        tweet['text']=''
    if tweet['user']['description']==None:
        tweet['user']['description']=''
    str=tweet['text']+tweet['user']['description']
    if flag!=0:
        if flag not in state_count.keys():
            state_count.update({flag:1})
        else:
            state_count.update({flag:state_count[flag]+1})
        if flag=='CA':
            city=tweet['place']['name']
            if city not in ca.keys():
                ca.update({city:1})
            else:
                ca.update({city:ca[city]+1})
    for ch in str:
        if ch in emoji.UNICODE_EMOJI:
            if ch not in allemoj.keys():
                allemoj.update({ch:1})
            else:
                allemoj.update({ch:allemoj[ch]+1})
            if flag == 0:
                continue
            addwordindict(emoj,ch,flag)
            addwordindict(state,flag,ch)

#map.save('map.html')
#-------------------------------------------------------------
#print(state),print(emoj)
#------------------------------------------------------------------------------------------
#PARTB 1. To Get the Top 15 emojis from the allemoj set
i=0
list=[]
for item in sortdic(allemoj):
    list.append(item)
    i=i+1
    if(i==15):
        break
#print(list)
#--------------------------------------------------------------------------------------
#PARTB 2. To Get the top 5 state for 🎄
list=emoj['🎄']
list=sortdic(list)
i=0
list1=[]
for item in list:
    list1.append(item)
    i=i+1
    if(i==5):
        break
#print(list1)
# -------------------------------------
#PARTB 3. To Get the top5 emojis for MA
list=state['MA']
list=sortdic(list)
i=0
list1=[]
for item in list:
    list1.append(item)
    i=i+1
    if(i==5):
        break
#print(list1)
# ----------------------------------------
#PARTB 4. To Get the top 5 states use emojis
topstate={}
for key1 in state.keys():
    num=0
    for key2 in state[key1].keys():
        num=num+state[key1][key2]
    topstate.update({key1:num})
topstate=sortdic(topstate)
i=0
list1=[]
for item in topstate:
    list1.append(item)
    i=i+1
    if(i==5):
        break
#print(list1)
# -----------------------------------------------

#PARTC 1. What are the top 5 states that have tweets
#PARTC 2. In the state of California, what are the top 5 cities that tweet?

'''ca=sortdic(ca)
state_count=sortdic(state_count)
for i in range(0,5):
    print(state_count[i])
for i in range(0,5):
    print(ca[i])'''
#extra credit
#------------------------------------------------------------------
for key1 in state:
    state[key1]=sortdic(state[key1])
#print(state)
new=tweets_iterator.rewind()
map2= folium.Map(location=[48, -102], zoom_start=13)
for tweet in new:
    #print(tweet)
    if tweet['place']==None or tweet['place']['full_name']==None:
       continue
    location=tweet['place']['full_name']
       #print(location)
    loc=location.split(',')
    flag=0
    for i in loc:
        #print(i)
        if i.strip() in state.keys():
            flag=i.strip()
            break
    if flag==0:
        continue
    if tweet['text']==None:
        tweet['text']=''
    if tweet['user']['description']==None:
        tweet['user']['description']=''
    str=tweet['text']+tweet['user']['description']
    for ch in str:
        if ch in emoji.UNICODE_EMOJI:
           if ch in state[flag][0][1] or ch in state[flag][1][1]:
            print(tweet['geo']['coordinates'])
            folium.CircleMarker(tweet['geo']['coordinates'],
                               radius=0.001,
                               popup='tweet site',
                               color='#3186cc',
                                  fill_color='#3186cc', ).add_to(map2)
map2.save('extra.html')









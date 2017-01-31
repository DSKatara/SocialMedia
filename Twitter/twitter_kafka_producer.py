#-*- coding: utf-8 -*-
#!/usr/bin/env python

import tweepy
import threading, logging, time
from kafka import KafkaProducer
import string
import twitter_utils
import simplejson as json

######################################################################
# Specify the topic name
######################################################################
mytopic='katarastream' # ex. 'twitterstream', or 'test' ...

######################################################################
# Create the Kafka payload
######################################################################

def createPayload(tweet):
#    print tweet
    payload = []
    print 'INSIDE TWEETS'
#    print 'autor id: ' + str(tweet.author.id_str)
    try:
        payload.append(str(tweet.author.id_str))
    except:
        payload.append('NULL')

#    print 'here'
    try:
        payload.append(twitter_utils.checkNull(tweet.author.screen_name))
    except:
        payload.append('NULL')

    try:
        payload.append(twitter_utils.checkNull(tweet.author.description))
    except:
        payload.append('NULL')

#    print 'after descrip'
    try:
        payload.append(str(tweet.author.favourites_count))
    except:
        payload.append('NULL')

    try:
        payload.append(str(tweet.author.followers_count))
    except:
        payload.append('NULL')

    try:
        payload.append(str(tweet.author.friends_count))
    except:
        payload.append('NULL')

    try:
        payload.append(str(tweet.author.listed_count))
    except:
        payload.append('NULL')

    try:
        payload.append(twitter_utils.checkNull(tweet.author.location))
    except:
        payload.append('NULL')

#    print 'after location'
    try:
        payload.append(str(tweet.author.id_str))
    except:
        payload.append('NULL')

    try:
        payload.append(twitter_utils.checkNull(tweet.author.time_zone))
    except:
        payload.append('NULL')

    try:
        payload.append(str(tweet.author.statuses_count))
    except:
        payload.append('NULL')

    try:
        payload.append(str(tweet.created_at.strftime('%Y-%m-%d  %H:%M:%S')))
    except:
        payload.append('NULL')

    try:
        payload.append(twitter_utils.checkNull(str(tweet.favorite_count)))
    except:
        payload.append('NULL')

    try:
        payload.append(str(tweet.id_str))
    except:
        payload.append('NULL')
#    print 'after tweet id'
    try:
        payload.append(twitter_utils.checkNull(str(tweet.in_reply_to_status_id_str)))
    except:
        payload.append('NULL')

    try:
        payload.append(twitter_utils.checkNull(str(tweet.in_reply_to_user_id_str)))
    except:
        payload.append('NULL')

    try:
        payload.append(twitter_utils.checkNull(str(tweet.lang)))
    except:
        payload.append('NULL')

    try:
        payload.append(twitter_utils.checkNull(tweet.possibly_sensitive))
    except:
        payload.append('NULL')
    try:
        payload.append(str(tweet.retweet_count))
    except:
        payload.append('NULL')

    try:
        payload.append(twitter_utils.checkNull(tweet.text))
    except:
        payload.append('NULL')

#    print 'after text ' + twitter_utils.checkNull(tweet.text)
    try:
        payload.append(twitter_utils.checkNull(str(tweet.entities["urls"][0]["url"])))
    except:
        payload.append('NULL')

#    print 'url1'
    try:
        payload.append(twitter_utils.checkNull(str(tweet.entities["urls"][0]["expanded_url"])))
    except:
        payload.append('NULL')

#    print 'url2'
    try:
        payload.append(twitter_utils.checkNull(str(tweet.entities["media"][0]["media_url"])))
    except:
        payload.append('NULL')

    sentiment = twitter_utils.getSentiment(twitter_utils.checkNull(tweet.text), tweet.lang)

#    print sentiment['disgust']
#    print "sentiment.disgust " + str(sentiment['disgust'])

    payload_str =  "`".join(payload) + '`' + str(sentiment['disgust']) + '`' + str(sentiment['fear']) + '`'  + \
                        str(sentiment['sadness']) + '`' + str(sentiment['surprise']) + '`' + str(sentiment['trust']) + '`' + \
                        str(sentiment['negative']) + '`' + str(sentiment['positive'])

    print payload_str

    return(payload_str)

######################################################################
#Create a handler for the streaming data that stays open...
######################################################################

class StdOutListener(tweepy.StreamListener):

    #Handler
    ''' Handles data received from the stream. '''

    ######################################################################
    #For each status event
    ######################################################################

    def on_status(self, message):

        #producer.send('twitterstream', msg)
        #print 'inside input'
        kafka_input = createPayload(message)
        #print 'input to kafka' + kafka_input
        try:
                print 'inside kafka'
                producer.send(mytopic.encode('utf-8'), kafka_input)

        except Exception,e:
                print("Exception while writing in kafka topic")
                return True
                twitter_utils.sendErrorMail('The error is: ' + e)
#       return True

    ######################################################################
    #Supress Failure to keep demo running... In a production situation
    #Handle with seperate handler
    ######################################################################

    def on_error(self, status_code):

        print('Got an error with status code: ' + str(status_code))
        twitter_utils.sendErrorMail('Got an error with status code: ' + str(status_code))
        return True # To continue listening

    def on_timeout(self):

        print('Timeout...')
        twitter_utils.sendErrorMail('Timeout Error from Twitter Listner')
        return True # To continue listening

######################################################################
#Main Loop Init
######################################################################


if __name__ == '__main__':

    try:
        print 'Beginning the flow'
        producer = KafkaProducer(bootstrap_servers='202.*.*.*:6667')

        listener = StdOutListener()

        #sign oath cert
        auth = tweepy.OAuthHandler(twitter_utils.consumer_key, twitter_utils.consumer_secret)

        #search_text = json.loads(ConfigSectionMap("search_parms")['search_text'].decode('utf-8'))
        #print search_text
        auth.set_access_token(twitter_utils.access_token, twitter_utils.access_secret)

        stream = tweepy.Stream(auth, listener)

        ######################################################################
        #Sample delivers a stream of 1% (random selection) of all tweets
        ######################################################################
        #track_var =  twitter_utils.streaming_track['tracks']
        #print track_var
        print 'streaming now'
        stream.filter(track=[unicode("APPLE","utf-8"),unicode("#CulturalEvent","utf-8"),unicode("كتارا_","utf-8")])

    except Exception,e:
        print 'Unknown Error in main loop'
        #twitter_utils.sendErrorMail('Error in main loop is: ' + str(e))
        print Exception,e
        twitter_utils.sendErrorMail('Error in main loop is: ' + str(e))

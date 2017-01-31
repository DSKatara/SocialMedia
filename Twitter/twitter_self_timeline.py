import tweepy #https://github.com/tweepy/tweepy
import csv
import time,datetime
import codecs, ConfigParser

# Function to access the access token stored within the configuration file
def ConfigSectionMap(section):
    Config = ConfigParser.ConfigParser()
    Config.read("/root/***/***")

    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

consumer_key    = ConfigSectionMap("auth_parms")['consumer_key']
consumer_secret = ConfigSectionMap("auth_parms")['consumer_secret']
access_key      = ConfigSectionMap("auth_parms")['access_token']
access_secret   = ConfigSectionMap("auth_parms")['access_secret']

file_name = "/root/katara-arabic/data/self_timeline/" + str(datetime.datetime.now().day) + str(datetime.datetime.now().month) + \
                str(datetime.datetime.now().year) + str(datetime.datetime.now().hour) + str(datetime.datetime.now().minute) + 'twitter_home_timeline'
#file_name = "/root/katara-arabic/data/self_timeline/twitter_home_timeline"
print file_name

target1 = codecs.open(file_name, 'a')

def writef(field,eof):
    if field is None:
        target1.write("NULL")
    else:
        #print field
        target1.write(field)
    if eof == 'N':
        #print 'inside eof1'
        target1.write("`")
    elif eof == 'Y':
        #print 'inside eof2'
        target1.write("\n")

def get_all_tweets(screen_name):
        #Twitter only allows access to a users most recent 3240 tweets with this method

        #authorize twitter, initialize tweepy
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_key, access_secret)
        api = tweepy.API(auth)

        #initialize a list to hold all the tweepy Tweets
        alltweets = []

        #make initial request for most recent tweets (200 is the maximum allowed count)
        new_tweets = api.user_timeline(screen_name = screen_name,count=200)

        #save most recent tweets
        alltweets.extend(new_tweets)

        #save the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1

        #keep grabbing tweets until there are no tweets left to grab
        while len(new_tweets) > 0:
                print "getting tweets before %s" % (oldest)

                #all subsiquent requests use the max_id param to prevent duplicates
                new_tweets = api.user_timeline(screen_name = screen_name,count=200,max_id=oldest)

                #save most recent tweets
                alltweets.extend(new_tweets)

                #update the id of the oldest tweet less one
                oldest = alltweets[-1].id - 1

                print "...%s tweets downloaded so far" % (len(alltweets))

        #transform the tweepy tweets into a 2D array that will populate the csv
        #outtweets = [[tweet.id_str, tweet.created_at, tweet.text.encode("utf-8")] for tweet in alltweets]
        for tweet in alltweets:
            print 'INSIDE TWEETS'
            writef(time.strftime("%Y-%m-%d"),'N')                   # date of capture
            writef(str(tweet.author.id_str),'N')                   # id of author
            try:
                writef(''.join(tweet.author.screen_name.encode('utf-8').splitlines()),'N')
            except UnicodeDecodeError, e:
                writef(''.join(tweet.author.screen_name.splitlines()),'N')
            except KeyError:
                writef(None,'N')
            try:
                writef(''.join(tweet.author.description.encode('utf-8').splitlines()),'N')       # description of the author
            except UnicodeDecodeError, e:
                writef(''.join(tweet.author.description.splitlines()),'N')
            except KeyError:
                writef(None,'N')
            writef(str(tweet.author.favourites_count),'N')         # number of tweets this user has favorited in the accounts lifetime
            writef(str(tweet.author.followers_count),'N')          # The number of followers this account currently has
            writef(str(tweet.author.friends_count),'N')            # The number of users this account is following AKA their followings
            writef(str(tweet.author.listed_count),'N')                     # The number of public lists that this user is a member of
            try:
                writef(''.join(tweet.author.location.encode('utf-8').splitlines()),'N')                          # The user-defined location for this accounts profile
            except UnicodeDecodeError, e:
                writef(''.join(tweet.author.location.splitlines()),'N')
            except KeyError:
                writef(None,'N')
            try:
                writef(tweet.author.time_zone,'N')                         # A string describing the Time Zone this user declares themselves within
            except KeyError, e:
                writef(None,'N')
            writef(str(tweet.author.statuses_count),'N')                   # The number of tweets (including retweets) issued by the user
            writef(tweet.created_at.strftime('%Y-%m-%d  %H:%M:%S'),'N')    # Time at which tweet was created
            try:
                writef(str(tweet.favorite_count),'N')                              # Indicates approximately how many times this Tweet has been liked by Twitter users
            except KeyError, e:
                writef(None,'N')
            writef(tweet.id_str,'N')                                       # The string representation of the unique identifier for this Tweet
            try:
                writef(tweet.in_reply_to_status_id_str,'N')                # If the represented Tweet is a reply, this is the string representation of the original Tweets$
            except KeyError, e:
                writef(None,'N')
            try:
                writef(tweet.in_reply_to_user_id_str,'N')                  # If the represented Tweet is a reply, this is the string of the original Tweet author ID
            except KeyError, e:
                writef(None,'N')
            try:
                writef(tweet.lang,'N')                                     # Language of tweet
            except KeyError, e:
                writef(None,'N')
            try:
                writef(str(tweet.possibly_sensitive),'N')                  # an indicator that the URL in tweet may contain content or media identified as sensitive conte$
            except (KeyError, AttributeError):
                writef(None,'N')
            writef(str(tweet.retweet_count),'N')                   # Number of times this Tweet has been retweeted
            try:
                writef(''.join(tweet.text.encode('utf-8').splitlines()),'N')
            except UnicodeDecodeError, e:
#               print e
#               print tweet.id_str
                print ''.join(tweet.text.splitlines())
                writef(''.join(tweet.text.splitlines()),'N')

            try:
                writef(str(tweet.entities["urls"][0]["url"]),'N')                  # The t.co URL that was extracted from the Tweet text
            except (KeyError, IndexError):
                writef(None,'N')
            try:
                writef(str(tweet.entities["urls"][0]["expanded_url"]),'N')         # The expanded URL
            except (KeyError, IndexError):
                writef(None,'N')
            try:
                writef(str(tweet.entities["media"][0]["media_url"]),'Y')   # The fully resolved media URL
            except KeyError, e:
                writef(None,'Y')


if __name__ == '__main__':
        #pass in the username of the account you want to download
        keyword="kataraqatar"
        get_all_tweets(keyword)

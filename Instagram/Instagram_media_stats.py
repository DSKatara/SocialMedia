import urllib2, json, requests, time, datetime, os, ConfigParser, codecs, io
from shutil import copyfile

print "Instagram "

# Function to access the access token stored within the configuration file
def ConfigSectionMap(section):
    Config = ConfigParser.ConfigParser()
    Config.read("/root/instagram/**/**.ini")

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

# Read the access token for using it in the program
access_token=ConfigSectionMap("access")['access_token']

# Read if full refresh or not
full_refresh_flag = ConfigSectionMap("refreshinfo")["full_refresh"]

# Set the run date
current_date = time.strftime('%Y-%m-%d %H:%M:%S') #current time, to keep a track on data capture date
capture_date = current_date

# Set the file names
post_file_name = "/root/instagram/history/media_stats/" + 'post_stats' + str(datetime.datetime.now().day) + str(datetime.datetime.now().month) + \
                str(datetime.datetime.now().year) + str(datetime.datetime.now().hour) + str(datetime.datetime.now().minute)

print 'postfile = ' + post_file_name

def get_data_first():
    try:
            d = urllib2.urlopen('https://api.instagram.com/v1/users/self/media/recent/?access_token='+access_token) #Instagram search API query
            d1 = json.load(d)
            return d1
    except Exception, e:
            print "Error get_data_first,exception block, return code " + str(d1['meta']['code'])
            return None

def get_data_next(next_url):
    try:
        d = urllib2.urlopen(next_url) #Instagram search API query
        d1 = json.load(d)
        print 'inside the data_next'
        return d1
    except Exception, e:
        print "Error get_data_next,exception block, return code " + str(d1['meta']['code'])
        return None


def write_post_data(d1, f):
    messages = d1['data']
    #print messages
    count = 0
    for message in d1['data']:
        username = message['user']['username'] #media owner name
        user_id = message['user']['id'] #media owner user id
        comments_count = message['comments']['count'] #media comments count

        created_time_raw = message['created_time'] #media created time
        created_time = datetime.datetime.fromtimestamp(int(created_time_raw)).strftime('%Y-%m-%d %H:%M:%S') #conversion of instagram timestamp to readable format

        media_id = message['id'] #media id
#        print media_id

        media_caption = message['caption']['text'].encode('utf-8') #media caption to identify the post
#       print media_caption
        likes_count = message['likes']['count'] #media likes count

        json.dump(capture_date,f)
        f.write('`')

        if 'username' in message['user']:
            json.dump(username,f)
            f.write('`')
        else:
            username = ''
            json.dump(username,f)
            f.write('`')

        if 'id' in message['user']:
            json.dump(user_id,f)
#           print user_id
            f.write('`')
        else:
            user_id = ''
            json.dump(user_id,f)
            f.write('`')

        if 'count' in message['comments']:
            json.dump(comments_count,f)
#           print comments_count
            f.write('`')
        else:
            comments_count = ''
            json.dump(comments_count,f)
            f.write('`')

        if 'created_time' in message:
            json.dump(created_time,f)
            f.write('`')
        else:
            created_time = ''
            json.dump(created_time,f)
            f.write('`')

        if 'id' in message:
            json.dump(media_id,f)
#           print media_id
            f.write('`')
        else:
            media_id = ''
            json.dump(media_id,f)
            f.write('`')

        if 'count' in message['likes']:
            json.dump(likes_count,f)
            f.write('`')
        else:
            likes_count = ''
            json.dump(likes_count,f)
            f.write('`')

        if 'text' in message['caption']:
            f.write(json.dumps(media_caption, ensure_ascii=False))
            f.write('\n')
        else:
            media_caption = ''
            json.dump(media_caption,f)
            f.write('\n')

        count = count + 1
        count1 = str(count)
        print "Total Posts= " +count1

class Scrape:
    def main(self):
        with open(post_file_name, 'w') as post_file:
            if full_refresh_flag == 'Y':
                print 'RUNNING FULL REFRESH'
                data1 = get_data_first()
                data_f = data1
                print 'first time code is ' + str(data1['meta']['code'])
                print data_f['pagination']
                if data1['meta']['code'] != 200 or not data1:
                    print "THERE WAS NO 'D' RETURNED.. OR THE RESPONSE CODE WAS " +  data1['meta']['code']
                else:
#                   write_config(data1['pagination']['next_url'])
                    write_post_data(data1, post_file)
#                   print data_f['pagination']
                try:
                    print 'inside try'
                    print data_f['pagination']['next_url']
                    while data_f['pagination']['next_url'] != 'None':
                        print 'trying the next page'
                        data2 = get_data_next(data_f['pagination']['next_url'])
                        print 'the next data is here'
#                       print data2['pagination']['next_url']
                        if data2['meta']['code'] != 200 or not data2:
                            print "THERE WAS NO 'D' RETURNED.. OR THE RESPONSE CODE WAS " +  data2['meta']['code']
                        else:
                            print 'writing for data2'
                            write_post_data(data2, post_file)
#                           write_config(data1['pagination']['next_url'])
                        data_f = data2
                except:
                    print 'No more pages, program ending smoothly'

                time.sleep(60)
                copyfile(post_file_name, '/root/instagram/landing_media_stats/media_stats')
                cp_to_hdfs = 'sudo -u root hadoop fs -copyFromLocal ' + post_file_name + ' /data/katara_data/instagram/media_stats/'
                os.system(cp_to_hdfs)
            else:
                print 'ILLEGAL RUN PARM - HAS TO BE Y'

if __name__ == "__main__":
    s = Scrape()
    s.main()

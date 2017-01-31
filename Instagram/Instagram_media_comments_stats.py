import urllib2, json, requests, time, datetime, os, re, ConfigParser, codecs

print "Instagram- Media Comments Stats"

# Function to access the access token stored within the configuration file
def ConfigSectionMap(section):
    Config = ConfigParser.ConfigParser()
    Config.read("/root/instagram/**/***.ini")

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

current_date = time.strftime("%Y-%m-%d"" ""%H:%M:%S") #current time, to keep a track on data capture date
capture_date = current_date

def get_data():
        separator = '`'
        for line in open('/root/instagram/landing_media_stats/media_stats'): #fetch media ID's from the media_stats dump
                columns = line.split(separator)
#                print columns
                if columns[3] > '0':
                        if len(columns) >= 2:
                                media_id = re.sub(r'^"|"$', '', columns[5]) #remove the double qoutes(" ") from the media id
                                print 'fetching comments for ' + media_id
                                print ('https://api.instagram.com/v1/media/' +media_id+ '/comments?access_token='+access_token)
                                d = urllib2.urlopen('https://api.instagram.com/v1/media/' +media_id+ '/comments?access_token='+access_token)
                                d1 = json.load(d)
                                write_data(d1,media_id)

file_name = '/root/instagram/history/media_comments/' + str(datetime.datetime.now().day) + str(datetime.datetime.now().month) + \
                str(datetime.datetime.now().year) + str(datetime.datetime.now().hour) + str(datetime.datetime.now().minute) + '_media_comments_stats'

print file_name

#target1 = open(file_name, 'w')
target1 = codecs.open(file_name, 'w', encoding='utf-8')

def writef(field,eof):
        if field is None:
                target1.write('NULL') #print NULL is not data found
        else:
                target1.write(field)
        if eof == 'N':
                target1.write('`')
        elif eof == 'Y':
                target1.write("\n")

def write_data(d1, media_id):
        print d1
        print media_id

        for message in d1['data']:
                print 'inside data'
                created_time_raw = message['created_time'] #comment post time
                created_time = datetime.datetime.fromtimestamp(int(created_time_raw)).strftime('%Y-%m-%d %H:%M:%S') #conversion of instagram timestamp to readable format
                comment_id = message['id']
                str = message['text']
                text = str.replace("`", "") #remove the delimeter character from the comments text to avoid improper loads in hive
#               print text
                comment_by = message['from']['username'] #comment owner
                comment_by_id = message['from']['id'] #comment owner id

                try:
                        writef(capture_date,'N')
                        writef(media_id,'N')
                        writef(comment_id,'N')
                        writef(created_time,'N')
                        writef(comment_by,'N')
                        writef(comment_by_id,'N')
                        writef(text,'Y')
                except KeyError, e:
                        writef(None,'N')


class Scrape:
        def main(self):
                data1 = get_data()
                if not data1:
                        print "\n"
                else:
                        write_data(data1)

                cp_to_hdfs = 'sudo -u root hadoop fs -copyFromLocal ' + file_name + ' /data/katara_data/*.../*..../'
                os.system(cp_to_hdfs)

if __name__ == "__main__":
        s = Scrape()
        s.main()

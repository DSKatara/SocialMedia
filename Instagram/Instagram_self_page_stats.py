import urllib2, json, requests, time, datetime, os, ConfigParser
import codecs

print "Instagram- ACCESS TOKEN OWNER STATS"

# Function to access the access token stored within the configuration file
def ConfigSectionMap(section):
    Config = ConfigParser.ConfigParser()
    Config.read("/root/instagram/c***/*****.ini")

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

def get_data():
    try:
        d = urllib2.urlopen('https://api.instagram.com/v1/users/self/?access_token='+access_token) #instagram search API query for access token owner details
        d1 = json.load(d)
        print d1
        return d1
    except Exception, e:
        print "Error get_data exception block"
        return None


file_name = "/root/instagram/history/self_page/" + str(datetime.datetime.now().day) + str(datetime.datetime.now().month) + \
                str(datetime.datetime.now().year) + str(datetime.datetime.now().hour) + str(datetime.datetime.now().minute) + '_self_page_stats' #generating file name with time stamp of capture

target1 = codecs.open(file_name, 'w')
print file_name

def writef(field,eof):
    if field is None:
        target1.write("NULL")
    else:
        print field
        target1.write(field)
    if eof == 'N':
        print 'inside eof1'
        target1.write("`")
    elif eof == 'Y':
        print 'inside eof2'
        target1.write("\n")

def write_data(d1,f):
    capture_date    = time.strftime("%Y-%m-%d"" ""%H:%M:%S") #current time, to keep a track on data capture date
    username        = d1['data']['username'] #username fetched from the search query
    full_name       = d1['data']['full_name'] #fullname of the access token owner
    id              = d1['data']['id'] #user id
    followed_by     = d1['data']['counts']['followed_by'] #number of followers
    follows         = d1['data']['counts']['follows'] #count of accounts followed by the owner
    media           = d1['data']['counts']['media'] #number of posts

#    with open(file_name,'w') as f:
    print 'inside the write for id' + str(id)
    json.dump(capture_date,f)
    f.write('`')
    #print "Capture Date= " +str(capture_date)
    if 'username' in d1['data']:
        f.write(username.encode('utf-8'))
#       json.dump(username.encode('utf-8'),f)
        f.write('`')
        print "User Name= " +str(username.encode('utf-8'))
    else:
        username = ''
        json.dump(username,f)
        f.write('`')
        print "User Name= " +str(username)

    if 'full_name' in d1['data']:
        json.dump(full_name.encode('utf-8'),f)
        f.write('`')
        print "Full Name= " +str(full_name.encode('utf-8'))
    else:
        full_name = ''
        json.dump(full_name,f)
        f.write('`')
        print "Full Name= " +str(full_name)

    if 'id' in d1['data']:
        json.dump(id,f)
        f.write('`')
        print "User ID= " +str(id)
    else:
        id = ''
        json.dump(id,f)
        f.write('`')
        print "User ID= " +str(id)

    if 'followed_by' in d1['data']['counts']:
        json.dump(followed_by,f)
        f.write('`')
        print "Followers Count= " +str(followed_by)
    else:
        followed_by = ''
        json.dump(followed_by,f)
        f.write('`')
        print "Followers Count= " +str(followed_by)

    if 'follows' in d1['data']['counts']:
        json.dump(follows,f)
        f.write('`')
        print "Follows Count= " +str(follows)
    else:
        follows = ''
        json.dump(follows,f)
        f.write('`')
        print "Follows Count= " +str(follows)

    if 'media' in d1['data']['counts']:
        json.dump(media,f)
        f.write('\n')
        print "Media/Post Count= " +str(media)
    else:
        follows = ''
        json.dump(media,f)
        f.write('\n')
        print "Media/Post Count= " +str(media)

class Scrape:
    def main(self):
        data1 = get_data()
#       print data1
        if not data1: #check for no entries in the search API
            print "THERE WAS NO 'D' RETURNED........MOVING TO NEXT ID"
        else:
            write_data(data1,target1)  #call write_data


if __name__ == "__main__":
    s = Scrape()
    s.main()

import facebook
import requests
import json
from datetime import datetime,date
import time
import datetime, os

access_token = '****************************'

graph = facebook.GraphAPI(access_token,version='2.5')
fb_page_id='182510261778259'

captured_at= time.strftime("%Y-%m-%d %H:%M:%S")

file_name = '/root/facebook/data/page_stats/' + str(datetime.datetime.now().day) + str(datetime.datetime.now().month) + \
                str(datetime.datetime.now().year) + str(datetime.datetime.now().hour) + str(datetime.datetime.now().minute) + 'fb_page_stats'

print file_name

target1 = open(file_name, 'w')

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


'''def get_fb_page_fan_count(fb_page_id):
    args = {'fields': 'likes'}
    page = graph.get_object(fb_page_id, **args)
    #return page.get('likes', 0)
    fan_count= page.get('likes', 0)
    json.dump(fan_count,f)
    f.write('`')'''

#fan_count = get_fb_page_fan_count(fb_page_id)
#print(fan_count)


def get_fb_page_insights(fb_page_id):
    print "Insights---------"
    args = {'fields': 'insights'}
    post_insights = graph.get_object(fb_page_id, **args)
    #print json.dumps(post_insights)
    insights_json=post_insights['insights']
    insights_data_json=insights_json['data']

    for page_insights in insights_data_json:
         #Daily: The number of new people who have liked your Page (Total Count)
        if page_insights['name'] == 'page_fans' and page_insights['period'] == 'lifetime':
            print page_insights['name']
            values=page_insights['values']
            writef(str(captured_at),'N')
            writef('page_fans','N')
            try:
                    values2=values[2]
                    end_time=values2['end_time']
                    sub_end_time=end_time[0:10]
                    value=values2['value']
                    #print sub_end_time
                    #print value
                    writef(str(sub_end_time),'N')
                    writef(str(value),'Y')
            except:
                    end_time=''
                    value=0
                    writef(str(end_time),'N')
                    writef(str(value),'Y')


         #Daily: The number of new people who have liked your Page (Total Count)
        if page_insights['name'] == 'page_fan_adds' and page_insights['period'] == 'day':
            print page_insights['name']
            values=page_insights['values']
            writef(str(captured_at),'N')
            writef('page_fan_adds','N')
            try:
                    values2=values[2]
                    end_time=values2['end_time']
                    sub_end_time=end_time[0:10]
                    value=values2['value']
                    #print sub_end_time
                    #print value
                    writef(str(sub_end_time),'N')
                    writef(str(value),'Y')
            except:
                    end_time=''
                    value=0
                    writef(str(end_time),'N')
                    writef(str(value),'Y')


        #Daily: The number of Unlikes of your Page (Total Count)
        if page_insights['name'] == 'page_fan_removes' and page_insights['period'] == 'day':
            print page_insights['name']
            values=page_insights['values']
            writef(str(captured_at),'N')
            writef('page_fan_removes','N')
            try:
                    values2=values[2]
                    end_time=values2['end_time']
                    sub_end_time=end_time[0:10]
                    value=values2['value']
                    #print sub_end_time
                    #print value
                    writef(str(sub_end_time),'N')
                    writef(str(value),'Y')
            except:
                    end_time=''
                    value=0
                    writef(str(end_time),'N')
                    writef(str(value),'Y')

         #Daily: The number of Unlikes of your Page (Total Count)
        if page_insights['name'] == 'page_engaged_users' and page_insights['period'] == 'day':
            print page_insights['name']
            values=page_insights['values']
            writef(str(captured_at),'N')
            writef('page_engaged_users ','N')
            try:
                    values2=values[2]
                    end_time=values2['end_time']
                    sub_end_time=end_time[0:10]
                    value=values2['value']
                    #print sub_end_time
                    #print value
                    writef(str(sub_end_time),'N')
                    writef(str(value),'Y')
            except:
                    end_time=''
                    value=0
                    writef(str(end_time),'N')
                    writef(str(value),'Y')

       #Daily: The number of impressions seen of any content associated with your Page. (Total Count)
        if page_insights['name'] == 'page_impressions' and page_insights['period'] == 'day':
            print page_insights['name']
            values=page_insights['values']
            writef(str(captured_at),'N')
            writef('page_impressions','N')
            try:
                    values2=values[2]
                    end_time=values2['end_time']
                    sub_end_time=end_time[0:10]
                    value=values2['value']
                    #print sub_end_time
                    #print value
                    writef(str(sub_end_time),'N')
                    writef(str(value),'Y')
            except:
                    end_time=''
                    value=0
                    writef(str(end_time),'N')
                    writef(str(value),'Y')



        #Daily: Total check-ins at your Place (Total Count)
        if page_insights['name'] == 'page_places_checkin_total' and page_insights['period'] == 'day':
            print page_insights['name']
            values=page_insights['values']
            writef(str(captured_at),'N')
            writef('page_places_checkin_total','N')
            try:
                    values2=values[2]
                    end_time=values2['end_time']
                    sub_end_time=end_time[0:10]
                    value=values2['value']
                    #print sub_end_time
                    #print value
                    writef(str(sub_end_time),'N')
                    writef(str(value),'Y')
            except:
                    end_time=''
                    value=0
                    writef(str(end_time),'N')
                    writef(str(value),'Y')




        #Daily: The number of clicks on any of your content. Stories generated without clicks on page content (e.g., liking the page in Timeline) are not included. (Total Count)
        if page_insights['name'] == 'page_consumptions' and page_insights['period'] == 'day':
            print page_insights['name']
            values=page_insights['values']
            writef(str(captured_at),'N')
            writef('page_consumptions','N')
            try:
                    values2=values[2]
                    end_time=values2['end_time']
                    sub_end_time=end_time[0:10]
                    value=values2['value']
                    #print sub_end_time
                    #print value
                    writef(str(sub_end_time),'N')
                    writef(str(value),'Y')
            except:
                    end_time=''
                    value=0
                    writef(str(end_time),'N')
                    writef(str(value),'Y')




        #Daily: The number of new subscriptions to your Page (Total Count)
        if page_insights['name'] == 'page_follower_adds' and page_insights['period'] == 'day':
            print page_insights['name']
            values=page_insights['values']
            writef(str(captured_at),'N')
            writef('page_follower_adds','N')
            try:
                    values2=values[2]
                    end_time=values2['end_time']
                    sub_end_time=end_time[0:10]
                    value=values2['value']
                    #print sub_end_time
                    #print value
                    writef(str(sub_end_time),'N')
                    writef(str(value),'Y')
            except:
                    end_time=''
                    value=0
                    writef(str(end_time),'N')
                    writef(str(value),'Y')
        #Daily: The number of new subscriptions to your Page (Total Count)
        if page_insights['name'] == 'page_follower_removes' and page_insights['period'] == 'day':
            print page_insights['name']
            values=page_insights['values']
            writef(str(captured_at),'N')
            writef('page_follower_removes','N')
            try:
                    values2=values[2]
                    end_time=values2['end_time']
                    sub_end_time=end_time[0:10]
                    value=values2['value']
                    #print sub_end_time
                    #print value
                    writef(str(sub_end_time),'N')
                    writef(str(value),'Y')
            except:
                    end_time=''
                    value=0
                    writef(str(end_time),'N')
                    writef(str(value),'Y')

get_fb_page_insights(fb_page_id)

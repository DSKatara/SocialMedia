import facebook
import requests
import json
from datetime import datetime,date
import datetime
import time

access_token = '**************************************'

graph = facebook.GraphAPI(access_token)
fb_page_id='****'
captured_at= time.strftime("%Y-%m-%d %H:%M:%S")
file_name = '/root/****/*****/page_stats_lang_country/' + str(datetime.datetime.now().day) + str(datetime.datetime.now().month) + \
                str(datetime.datetime.now().year) + str(datetime.datetime.now().hour) + str(datetime.datetime.now().minute) + 'fb_page_stats_lan_country'
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



def get_fb_page_insights(fb_page_id):
    print "****Insights****"
    args = {'fields': 'insights'}
    post_insights = graph.get_object(fb_page_id, **args)
    #print json.dumps(post_insights)
    insights_json=post_insights['insights']
    insights_data_json=insights_json['data']

    for page_insights in insights_data_json:


        #Lifetime: Gender of people who like your Page (Unique Users)
        if page_insights['name'] == 'page_fans_gender' and page_insights['period'] == 'lifetime':
            print page_insights['name']
            values=page_insights['values']
            values2=values[2]
            print values
            try:
                    end_time=values2['end_time']
                    sub_end_time=end_time[0:10]
                    print sub_end_time

            except:
                        end_time=''
            try:
                        vals=values2['value']
                        print vals
                        writef(str(captured_at),'N')
                        writef('page_fans_gender','N')
                        writef(sub_end_time,'N')
                        writef(str(vals),'Y')

            except:
                        vals=0
                        writef(str(captured_at),'N')
                        writef('page_fans_gender','N')
                        writef(end_time,'N')
                        writef(str(vals),'Y')


        #Lifetime: Aggregated Facebook location data, sorted by country, about the people who like your Page. (Unique Users)
        if page_insights['name'] == 'page_fans_country' and page_insights['period'] == 'lifetime':
            print page_insights['name']
            values=page_insights['values']
            values2=values[2]
            #print values2
            try:
                    end_time=values2['end_time']
                    sub_end_time=end_time[0:10]
                    print sub_end_time

            except:
                        end_time=''
            try:
                vals=values2['value']
                for key, value in vals.items():
                        writef(str(captured_at),'N')
                        writef('page_fans_country','N')
                        writef(sub_end_time,'N')
                        key1=key
                        #print key1
                        value1=value
                        #print value1
                        writef(str(key1),'N')
                        writef(str(value1),'Y')
            except:
                        writef(str(captured_at),'N')
                        writef('page_fans_country','N')
                        writef(end_time,'N')
                        value=0
                        key=0
                        writef(str(key),'N')
                        writef(str(value),'Y')


        #Lifetime: Aggregated Facebook location data, sorted by city, about the people who like your Page. (Unique Users)
        if page_insights['name'] == 'page_fans_city' and page_insights['period'] == 'lifetime':
            print page_insights['name']
            values=page_insights['values']
            values2=values[2]
            #print values2
            try:
                    end_time=values2['end_time']
                    sub_end_time=end_time[0:10]
                    print sub_end_time

            except:
                        end_time=''
            try:
                vals=values2['value']
                for key, value in vals.items():
                        writef(str(captured_at),'N')
                        writef('page_fans_city','N')
                        writef(sub_end_time,'N')
                        key1=key
                        #print key1
                        value1=value
                        #print value1
                        writef(str(key1),'N')
                        writef(str(value1),'Y')
            except:
                        writef(str(captured_at),'N')
                        writef('page_fans_city','N')
                        writef(end_time,'N')
                        value=0
                        key=0
                        writef(str(key),'N')
                        writef(str(value),'Y')



        #Lifetime: Aggregated language data about the people who like your Page based on the default language setting selected when accessing Facebook. (Unique Users)
        if page_insights['name'] == 'page_fans_locale' and page_insights['period'] == 'lifetime':
                print page_insights['name']
                #for values in page_insights['values']:
                values=page_insights['values']
                values2=values[2]
                #print values2
                try:
                        end_time=values2['end_time']
                        sub_end_time=end_time[0:10]
                        print sub_end_time

                except:
                        end_time=''
                try:
                    vals=values2['value']
                    for key, value in vals.items():
                        writef(str(captured_at),'N')
                        writef('page_fans_locale','N')
                        writef(sub_end_time,'N')
                        key1=key
                        #print key1
                        value1=value
                        #print value1
                        writef(str(key1),'N')
                        writef(str(value1),'Y')
                except:
                        writef(str(captured_at),'N')
                        writef('page_fans_locale','N')
                        writef(end_time,'N')
                        value=0
                        key=0
                        writef(str(key),'N')
                        writef(str(value),'Y')

        #Daily: The number of people who have given negative feedback to your Page, by type. (Unique Users)
        if page_insights['name'] == 'page_negative_feedback_by_type' and page_insights['period'] == 'day':
                print page_insights['name']
                #for values in page_insights['values']:
                values=page_insights['values']
                values2=values[2]
                #print values2
                try:
                        end_time=values2['end_time']
                        sub_end_time=end_time[0:10]
                        print sub_end_time

                except:
                        end_time=''
                try:
                    vals=values2['value']
                    for key, value in vals.items():
                        writef(str(captured_at),'N')
                        writef('page_negative_feedback_by_type','N')
                        writef(sub_end_time,'N')
                        key1=key
                        #print key1
                        value1=value
                        #print value1
                        writef(str(key1),'N')
                        writef(str(value1),'Y')
                except:
                        writef(str(captured_at),'N')
                        writef('page_negative_feedback_by_type','N')
                        writef(end_time,'N')
                        value=0
                        key=0
                        writef(str(key),'N')
                        writef(str(value),'Y')


get_fb_page_insights(fb_page_id)

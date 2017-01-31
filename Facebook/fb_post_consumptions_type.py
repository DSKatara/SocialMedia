import facebook
import requests
import json
import datetime,time,os

access_token = '**************'

# Look at Users profile for this example by using his Facebook id.
fb_page_id = '************'
graph = facebook.GraphAPI(access_token)
captured_at= time.strftime("%Y-%m-%d"" ""%H:%M:%S")
file_name = '/root/facebook/data/post_stats_consumptions_type/' + str(datetime.datetime.now().day) + str(datetime.datetime.now().month) + \
                str(datetime.datetime.now().year) + str(datetime.datetime.now().hour) + str(datetime.datetime.now().minute) + 'fb_post_stats_consumption'
target1 = open(file_name, 'w')

def writef(field,eof):
    if field is None:
        value=0
        target1.write(str(value))
    else:
        print 'writing' + field
        target1.write(field)
    if eof == 'N':
        target1.write("`")
    elif eof == 'Y':
        target1.write("\n")

def get_fb_post_insights(fb_post_id):
    print "****Insights****"
    args = {'fields': 'insights'}
    post_insights = graph.get_object(fb_post_id, **args)
#    post_insights = graph.get_object(fb_post_id)
    print 'inside bhiya'
    insights_json=post_insights['insights']
    insights_data_json=insights_json['data']

    # "post_impressions post_story_add post_impressions_organic post_impressions_viral post_impressions_fan
    # post_reactions_like_total post_reactions_love_total post_reactions_wow_total post_reactions_haha_total post_reactions_sorry_total
    for post_insights in insights_data_json:
        #print post_insights
        if post_insights['name'] == 'post_consumptions_by_type' and post_insights['period'] == 'lifetime':
            #print post_insights['name']
            values=post_insights['values']
            json_val = values[0]
            if values[0].get('value'):
                print  values[0]
                writef(str(captured_at),'N')
                writef(str(post_id),'N')
                #writef('post_consumptions_by_type','N')
                try:
                    other_clicks = values[0]['value']["other clicks"]
                    #writef(str('other_clicks'),'N')
                    writef(str(other_clicks),'N')       # description of the author
                except KeyError, e:
                    #writef(str('other_clicks'),'N')
                    writef(None,'N')
                try:
                    video_play = values[0]['value']["video play"]
                    #writef(str('video_play'),'N')
                    writef(str(video_play),'N')       # description of the author
                except KeyError, e:
                    #writef(str('video_play'),'N')
                    writef(None,'N')
                try:
                    link_clicks = values[0]['value']["link clicks"]
                    #writef(str('link_clicks'),'N')
                    writef(str(link_clicks),'N')       # description of the author
                except KeyError, e:
                    #writef(str('link_clicks'),'N')
                    writef(None,'N')
                try:
                    photo_view = values[0]['value']["photo view"]
                    #writef(str('photo_view'),'N')
                    writef(str(photo_view),'Y')       # description of the author
                except KeyError, e:
                    #writef(str('photo_view'),'N')
                    writef(None,'Y')

profile = graph.get_object(id='*****')
posts = graph.get_connections(profile['id'], 'posts',limit='100')
postf = json.dumps(posts)
json_postdata = json.loads(postf)
json_fbposts = json_postdata['data']


for post in json_fbposts:
             #writef(str(captured_at),'N')
             post_id=post["id"]
             try:
                get_fb_post_insights(post_id)

             except Exception,e:
                #value=''
                #writef(str(value),'N')
                print Exception,e

import facebook
import requests
import json
import datetime,time,os
import codecs

access_token = '****************************'

# Look at Users profile for this example by using his Facebook id.
fb_page_id = '**************'

graph = facebook.GraphAPI(access_token,version='2.5')

captured_at= time.strftime("%Y-%m-%d"" ""%H:%M:%S")

file_name =  '/root/facebook/data/post_stats/' + str(datetime.datetime.now().day) + str(datetime.datetime.now().month) + \
                str(datetime.datetime.now().year) + str(datetime.datetime.now().hour) + str(datetime.datetime.now().minute) + 'fb_post_stats'

file_name1 =  '/root/facebook/data/post_comments_stats/' + str(datetime.datetime.now().day) + str(datetime.datetime.now().month) + \
                str(datetime.datetime.now().year) + str(datetime.datetime.now().hour) + str(datetime.datetime.now().minute) + 'fb_post_comments'

print file_name
print file_name1

target1 = codecs.open(file_name, 'w')
target2 = codecs.open(file_name1, 'w')

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

def writec(field,eof):
    if field is None:
        target2.write("NULL")
    else:
        print field
        target2.write(field)
    if eof == 'N':
        print 'inside eof1'
        target2.write("`")
    elif eof == 'Y':
        print 'inside eof2'
        target2.write("\n")

def get_fb_post_like_count(fb_post_id):
    post_likes = graph.get_connections(post['id'], connection_name='likes',summary='true',limit='1000')
    print "****Likes*****"
    post_likesl=post_likes['summary']
    if 'total_count' in post_likesl:
        post_likesj=post_likesl['total_count']
        #print post_likesj
        return post_likesj
    else:
        post_likesj=0
        return post_likesj



def get_fb_post_comment_count(fb_post_id):
    print "****Comments*****"
    post_comments = graph.get_connections(post['id'], connection_name='comments',summary='true',limit='1000')
    post_commentsl=post_comments['summary']
    if 'total_count' in post_commentsl:
        post_commentsj=post_commentsl['total_count']
        return post_commentsj
    else:
        post_commentsj=0
        return post_commentsj


def get_fb_post_share_count(fb_post_id):
    print "****Shares*****"
    args = {'fields': 'shares'}
    post_shares = graph.get_object(fb_post_id, **args)
    if 'shares' in post_shares:
        post_shares1=post_shares['shares']
        post_sharesc=post_shares1['count']
        return post_sharesc
    else:
        post_sharesc=0
        return post_sharesc



def get_fb_post_insights(fb_post_id):
    print "****Insights****"
    args = {'fields': 'insights'}
    post_insights = graph.get_object(fb_post_id, **args)
    insights_json=post_insights['insights']
    insights_data_json=insights_json['data']

    # "post_impressions post_story_add post_impressions_organic post_impressions_viral post_impressions_fan
    # post_reactions_like_total post_reactions_love_total post_reactions_wow_total post_reactions_haha_total post_reactions_sorry_total
    for insights in insights_data_json:

     if insights['name'] == 'post_story_adds' and insights['period'] == 'lifetime':
         print "post_story_adds"
         for values in insights['values']:
          try:
            value=values['value']
            #writef(str("post_story_adds"),'N')
            writef(str(value),'N')
          except:
            value=0
            #writef(str(post_story_adds),'N')
            writef(str(value),'N')

     if insights['name'] == 'post_impressions' and insights['period'] == 'lifetime':
         print "post_impressions"
         for values in insights['values']:
          try:
            value=values['value']
            #writef(str("post_impressions"),'N')
            writef(str(value),'N')
          except:
            value=0
           # writef(str(post_impressions),'N')
            writef(str(value),'N')

     if insights['name'] == 'post_impressions_organic' and insights['period'] == 'lifetime':
         print "post_impressions_organic"
         for values in insights['values']:
          try:
            value=values['value']
            #writef(str(post_impressions_organic),'N')
            writef(str(value),'N')
          except:
            value=0
            #writef(str(post_impressions_organic),'N')
            writef(str(value),'N')

     if insights['name'] == 'post_impressions_viral' and insights['period'] == 'lifetime':
         print "post_impressions_viral"
         for values in insights['values']:
          try:
            value=values['value']
            #writef(str(post_impressions_viral),'N')
            writef(str(value),'N')
          except:
            value=0
            #writef(str(post_impressions_viral),'N')
            writef(str(value),'N')

     if insights['name'] == 'post_impressions_fan' and insights['period'] == 'lifetime':
         print "post_impressions_fan"
         for values in insights['values']:
          try:
            value=values['value']
            #writef(str(post_impressions_fan),'N')
            writef(str(value),'N')
          except:
            value=0
            #writef(str(post_impressions_fan),'N')
            writef(str(value),'N')

     if insights['name'] =='post_engaged_users' and insights['period'] == 'lifetime':
         print "post_engaged_users"
         for values in insights['values']:
          try:
            value=values['value']
            #writef(str(post_engaged_users),'N')
            writef(str(value),'N')
          except:
            value=0
            #writef(str(post_engaged_users),'N')
            writef(str(value),'N')

     if insights['name'] == 'post_consumptions' and insights['period'] == 'lifetime':
         print "post_consumptions"
         for values in insights['values']:
          try:
            value=values['value']
            writef(str(value),'N')
          except:
            value=0
            writef(str(value),'N')

     if insights['name'] == 'post_negative_feedback_unique' and insights['period'] == 'lifetime':
         print "post_negative_feedback_unique"
         for values in insights['values']:
          try:
            value=values['value']
            #writef(str(post_negative_feedback_unique),'N')
            writef(str(value),'N')
          except:
            value=0
            #writef(str(post_negative_feedback_unique),'N')
            writef(str(value),'N')


     if insights['name'] == 'post_reactions_like_total' and insights['period'] == 'lifetime':
         print "post_reactions_like_total"
         for values in insights['values']:
          try:
            value=values['value']
            #writef(str(post_reactions_like_total),'N')
            writef(str(value),'N')
          except:
            value=0
            #writef(str(post_reactions_like_total),'N')
            writef(str(value),'N')

     if insights['name'] == 'post_reactions_love_total' and insights['period'] == 'lifetime':
         print "post_reactions_love_total"
         for values in insights['values']:
          try:
            value=values['value']
            #writef(str(post_reactions_love_total),'N')
            writef(str(value),'N')
          except:
            value=0
            #writef(str(post_reactions_love_total),'N')
            writef(str(value),'N')

     if insights['name'] == 'post_reactions_wow_total' and insights['period'] == 'lifetime':
         print "post_reactions_wow_total"
         for values in insights['values']:
          try:
            value=values['value']
            #writef(str(post_reactions_wow_total),'N')
            writef(str(value),'N')
          except:
            value=0
            #writef(str(post_reactions_wow_total),'N')
            writef(str(value),'N')

     if insights['name'] == 'post_reactions_haha_total' and insights['period'] == 'lifetime':
         print "post_reactions_haha_total"
         for values in insights['values']:
          try:
            value=values['value']
            #writef(str(post_reactions_haha_total),'N')
            writef(str(value),'N')
          except:
            value=0
            #writef(str(post_reactions_haha_total),'N')
            writef(str(value),'N')

     if insights['name'] == 'post_reactions_sorry_total' and insights['period'] == 'lifetime':
         print "post_reactions_sorry_total"
         for values in insights['values']:
          try:
            value=values['value']
            #writef(str(post_reactions_sorry_total),'N')
            writef(str(value),'N')
          except:
            value=0
            #writef(str(post_reactions_sorry_total),'N')
            writef(str(value),'N')

     if insights['name'] == 'post_reactions_anger_total' and insights['period'] == 'lifetime':
         print "post_reactions_anger_total"
         for values in insights['values']:
          try:
            value=values['value']
            #writef(str(post_reactions_anger_total),'N')
            writef(str(value),'N')
          except:
            value=0
            #writef(str(post_reactions_anger_total),'N')
            writef(str(value),'N')


def get_fb_post_comment_details(post_id):

        post_comments = graph.get_connections(post_id, connection_name='comments',summary='true',limit='1000')

        post_comment_data = post_comments['data']

        for comments in post_comment_data:

                writec(str(captured_at),'N')

                created_time=comments['created_time']
                sub_created_time=created_time[0:10]
                writec(str(sub_created_time),'N')

                writec(str(post_id),'N')

                id1=comments['id']
                writec(str(id1),'N')

                user=comments['from']
                username=user['name']
                try:
                        writec(''.join(username.encode('utf-8').splitlines()),'N')
                except UnicodeDecodeError, e:
                        writec(''.join(username.splitlines()),'N')

                id2=user['id']
                writec(str(id2),'N')

                message = comments['message']
                try:
                        writec(''.join(message.encode('utf-8').splitlines()),'Y')
                except UnicodeDecodeError, e:
                        writec(''.join(message.splitlines()),'Y')

profile = graph.get_object(id='******')
posts = graph.get_connections(profile['id'], 'posts',limit='100')
postf = json.dumps(posts)
json_postdata = json.loads(postf)
json_fbposts = json_postdata['data']


for post in json_fbposts:
             writef(str(captured_at),'N')
             try:
                if 'created_time' in post:
                  created_time=post["created_time"]
                  sub_created_time=created_time[0:10]
                  writef(str(sub_created_time),'N')
                else:
                  created_time=""
                  writef(str(created_time),'N')

                if 'id' in post:
                  post_id=post["id"]
                  writef(str(post_id),'N')
                else:
                  post_id=""
                  writef(str(post_id),'N')

                likes=get_fb_post_like_count(post_id)
                writef(str(likes),'N')
                comments=get_fb_post_comment_count(post_id)
                writef(str(comments),'N')
                shares=get_fb_post_share_count(post_id)
                writef(str(shares),'N')


                get_fb_post_insights(post_id)

                if 'message' in post:
                  message = post["message"]
                  try:
                        print "Inside try"
                        print(''.join(message.encode('utf-8').splitlines()))
                        writef(''.join(message.encode('utf-8').splitlines()),'Y')
                  except UnicodeDecodeError, e:
                        print "Inside try"
                        print(''.join(message.splitlines()))
                        writef(''.join(message.splitlines()),'Y')
                else:
                  message=""
                  writef(str(message),'Y')

                if comments > 0:
                        get_fb_post_comment_details(post_id)

             except Exception,e:
                #value=' '
                #writef(str(value),'Y')
                print Exception,e

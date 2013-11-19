#!/usr/bin/python
#-*-coding:utf8-*-

from pprint import pprint
from weibopy.auth import OAuthHandler
from weibopy.api import API
from weibopy.binder import bind_api
from weibopy.error import WeibopError
import time,os,pickle,sys
import logging.config 
from multiprocessing import Process
import os
import ujson as json
import time
import gzip
import sys, getopt


class Weibo_reptile():

    def __init__(self,consumer_key, consumer_secret, json_path, email):
        
        self.extra_sleep_time = 0
        self.consumer_key, self.consumer_secret = consumer_key, consumer_secret
        self.json_path = json_path
        self.email = email

    def getAtt(self, key):

        try:
            return self.obj.__getattribute__(key)

        except Exception, e:
            print e
            return ''

    def getAttValue(self, obj, key):

        try:
            return obj.__getattribute__(key)
        except Exception, e:
            print e
            return ''

    def auth(self):

        if len(self.consumer_key) == 0:
            print "Please set consumer_key"
            return
        
        if len(self.consumer_key) == 0:
            print "Please set consumer_secret"
            return
        
        self.auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        auth_url = self.auth.get_authorization_url()
        print 'Please authorize: ' + auth_url
        verifier = raw_input('PIN: ').strip()
        self.auth.get_access_token(verifier)
        self.api = API(self.auth)

    def setToken(self, token, tokenSecret):

        self.auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.setToken(token, tokenSecret)
        self.api = API(self.auth)

    def get_userprofile(self, id):

        try:
            userprofile = {}
            userprofile['id'] = id
            user = self.api.get_user(id)
            self.obj = user
            
            userprofile['screen_name'] = self.getAtt("screen_name")
            userprofile['name'] = self.getAtt("name")
            userprofile['province'] = self.getAtt("province")
            userprofile['city'] = self.getAtt("city")
            userprofile['location'] = self.getAtt("location")
            userprofile['description'] = self.getAtt("description")
            userprofile['url'] = self.getAtt("url")
            userprofile['profile_image_url'] = self.getAtt("profile_image_url")
            userprofile['domain'] = self.getAtt("domain")
            userprofile['gender'] = self.getAtt("gender")
            userprofile['followers_count'] = self.getAtt("followers_count")
            userprofile['friends_count'] = self.getAtt("friends_count")
            userprofile['statuses_count'] = self.getAtt("statuses_count")
            userprofile['favourites_count'] = self.getAtt("favourites_count")
            userprofile['created_at'] = self.getAtt("created_at")

        except WeibopError, e:      

            print "error occured when access userprofile use user_id:",id
            print "Error:",e
            log.error("Error occured when access userprofile use user_id:{0}\nError:{1}".format(id, e),exc_info=sys.exc_info())
            return None
            
        return userprofile


    def get_latest_weibo(self, user_id, count):

        status = []
        try:            #error occur in the SDK
            timeline = self.api.user_timeline(count = count, user_id = user_id)

        except Exception as e:

            print "error occured when access status use user_id:",user_id
            print "Error:",e
            log.error("Error occured when access status use user_id:{0}\nError:{1}".format(user_id, e),exc_info=sys.exc_info())
            return None

        for line in timeline:
            self.obj = line
            statusprofile = {}
            statusprofile['usr_id'] = user_id
            statusprofile['user'] = self.getAtt("user")
            statusprofile['id'] = self.getAtt("id")
            statusprofile['created_at'] = self.getAtt("created_at")
            statusprofile['text'] = self.getAtt("text")
            statusprofile['source'] = self.getAtt("source")
            statusprofile['favorited'] = self.getAtt("favorited")
            statusprofile['in_reply_to_status_id'] = self.getAtt("in_reply_to_status_id")
            statusprofile['in_reply_to_user_id'] = self.getAtt("in_reply_to_user_id")
            statusprofile['in_reply_to_screen_name'] = self.getAtt("in_reply_to_screen_name")
            statusprofile['geo'] = repr(pickle.dumps(self.getAtt("geo"),pickle.HIGHEST_PROTOCOL))
            statusprofile['mid'] = self.getAtt("mid")
            status.append(statusprofile)

        return status


    def friends_ids(self,id):

        next_cursor,cursor = 1,0
        ids = []
        while(0 != next_cursor):
            fids = self.api.friends_ids(user_id=id, cursor=cursor)
            self.obj = fids
            ids.extend(self.getAtt("ids"))
            cursor = next_cursor = self.getAtt("next_cursor")
            previous_cursor = self.getAtt("previous_cursor")
        return ids

    def manage_access(self):

        try:
            info = self.api.rate_limit_status()
            self.obj = info
            sleep_time = round( (float)(self.getAtt("reset_time_in_seconds")) / self.getAtt("remaining_hits"), 2) if self.getAtt("remaining_hits") else self.getAtt("reset_time_in_seconds")
            print "remining hits: " + str(self.getAtt("remaining_hits")) + ", reset time in secondes: ", str(self.getAtt("reset_time_in_seconds")) + ", hourly limit: " +  str(self.getAtt("hourly_limit")) + ", reset time: " + str(self.getAtt("reset_time"))
            print "sleep time: ", sleep_time, 'pid: ', os.getpid()
            print "total sleep time: ", sleep_time + self.extra_sleep_time, 'pid: ', os.getpid()
            time.sleep(sleep_time + self.extra_sleep_time)

        except Exception as e:

            if "invalid" in str(e):
                print 'Apk ' + self.consumer_key + ' doesn\'t work!' 
                invalid_key_file = file("invalid_key.txt", "ab+")
                invalid_key_file.write(self.consumer_key + "\n")
                invalid_key_file.close() 
                
                #send Email to the user
                if self.email:
                    content = "An apk has failed\n"

                    f = file("apk.txt")
                    f2 = file("invalid_key.txt")
                    num_key = 0

                    invalid_list = []
                    for invalid_id in f2.readlines():
                        invalid_list.append(invalid_id.rstrip())

                    for apk_line in f.readlines():
                        apk_strip = apk_line.strip().split(' ')
                        if apk_strip[0] not in invalid_list:
                            num_key += 1

                    content += "Still have " + str(num_key) + " keys left" 
                    self.send_email(content)


    """
        You can get the Email address with self.email
        Email content from the parameter content
    """
    def send_email(self, message):
        
        print 'Sending email: %s' %message 
        command = 'echo "%s" | mail -s "SUBJECT_LINE" %s' % (message, self.email)
        os.system(command)

    def save_to_json(self, userprofile, statuses):

        if self.json_file.rfind('/') == -1:
            file_name = time.strftime("%Y_%m_%d") + '.json' 

        else:
            file_name = self.json_file[0: self.json_path.rfind('/') + 1] + time.strftime("%Y_%m_%d") + '.json'

        json_file = file(file_name, "ab+") 
        for status in statuses:
            #json_object = json.dumps(status, ensure_ascii = False).encode('utf-8')
            json_object = json.dumps(status)
            json_file.write(json_object)
            json_file.write("\n")
        json_file.close()

    def save_to_gzip(self, userprofile, statuses):

        if self.json_path.rfind('/') == -1:
            directory = time.strftime("%Y/%m/")
            file_name = directory + time.strftime("%Y_%m_%d") + '.json.gz'

        else:
            directory = self.json_path[0: self.json_path.rfind('/') + 1] + time.strftime("%Y/%m/")  
            file_name = directory + time.strftime("%Y_%m_%d") + '.json.gz'

        if not os.path.exists(directory):
            os.makedirs(directory)

        gzip_out = gzip.open(file_name, "ab+") 

        for status in statuses:
            #json_object = json.dumps(status, ensure_ascii = False).encode('utf-8')
            json_object = json.dumps(status)
            gzip_out.write(json_object)
            gzip_out.write("\n")
        gzip_out.close()

    def save_data(self, userprofile, status):

        self.collection_statuses.insert(status)
        self.collection_userprofile.insert(userprofile)

"""
    The method I used for crawling users's data is as follows:
    It will start from a user who have lots of friends. Use the BFS algorithm to get other uesr's status.  
    I use a list "user_queue" to stores the next user that needs to crawl. At first, it have one user. 
    "visited_queue" will store all users that have been visited. 
"""    
def reptile(weibo_reptile, userid):

    number = 1                # This variable store the number of users that has crawled
    user_queue = [userid]     # This list functioned as a queue. It stores the next userid that needs to crawl
    visited_queue = user_queue

    for id in user_queue:
        try:
            weibo_reptile.manage_access()
            return_ids = weibo_reptile.friends_ids(id)

            userprofile = weibo_reptile.get_userprofile(id)
            status = weibo_reptile.get_latest_weibo(count = 100, user_id = id)

            if status is None or userprofile is None:
                continue

            print 'Already got the data weibo. Save the data now.'

            #Save the data to gzip
            weibo_reptile.save_to_gzip(userprofile, status)

        except Exception as e:
            #log.error("Error occured in reptile,id:{0}\nError: {1}".format(id, e), exc_info = sys.exc_info())
            print e
            time.sleep(60)
            continue

        number += 1
        visited_queue.append(id)
        print 'Number of users for pid: ' + str(os.getpid()) + ' has cralwed: ' + str(number) 

        # Remove that user from user_queue, and add the user that hasn't been visited in the user_queue
        user_queue.remove(id)

        # In order to be more efficient, I use pyhon's set operation, which will add the element that doesn't occur in 
        # visited_queue and user_queue into the user_queue 
        return_ids = list(set(return_ids) - set(visited_queue) - set(user_queue))
        user_queue.extend(return_ids)

def run_crawler(consumer_key, consumer_secret, key, secret, userid, json_path, email = None):

    try:
        #print 'consumer key: ' + consumer_key
        weibo_reptile = Weibo_reptile(consumer_key, consumer_secret, json_path, email)
        weibo_reptile.setToken(key, secret)
        reptile(weibo_reptile, userid)
        weibo_reptile.connection.close()

    except Exception as e:
        print e
        #log.error("Error occured in run_crawler, pid: {1}\nError: {2}".format(os.getpid(), e), exc_info = sys.exc_info())


def main():

    json_path = None 
    pid_file = None 
    email = None
    num_thread = None 
    argv = sys.argv[1:]

    try:
        opts, args = getopt.getopt(argv, "hf:p:n:e:")

    except getopt.GetoptError:
        print """
Usage: %s -f filepath [-p pid_file] [-e email] -n [number of threads] 
""" % (sys.argv[0])
        sys.exit(1)

    for opt, arg in opts:

        if opt == '-h':
            print """
Usage: %s -f filepath [-p pid_file] [-e email] -n [number of threads] 
""" % (sys.argv[0])
            sys.exit(1)

        elif opt in ("-f"):
            json_path = arg

        elif opt in ("-p"):
            pid_file = arg

        elif opt in ("-e"):
            email = arg

        elif opt in ("-n"):
            num_thread = arg

    if json_path == None or num_thread == None: 
        print """
Usage: %s -f filepath [-p pid_file] [-e email] -n [number of threads] 
""" % (sys.argv[0])
        sys.exit(1)

    print "Store json.gz in " + json_path + " directory" 
    if pid_file != None:
        print "Store pid file " + pid_file
    print "Num of threads: " + str(num_thread)

    num = 0

    #create a pid file
    if pid_file != None:
        pid = str(os.getpid())
        '''
        if os.path.isfile(pid_file):
            print "%s already exists, exiting. Please delete it before running." % pid_file
            sys.exit()

        else:
        '''
        file(pid_file, 'w').write(pid)
            
    # Pick correct URL root to use
    logging.config.fileConfig("logging.conf")
    with open('apk.txt') as f, open('invalid_key.txt', 'ab+') as f2:
        invalid_list = []
        for invalid_id in f2.readlines():
            invalid_list.append(invalid_id.rstrip())

        for apk_line in f.readlines():
            if num >= int(num_thread):
                break 
            
            apk_strip = apk_line.strip().split(' ')
            if apk_strip[0] in invalid_list:
                continue 

            p = Process(target = run_crawler, args=(apk_strip[0], apk_strip[1], apk_strip[2], apk_strip[3], apk_strip[4], json_path, email))
            p.start()
            num += 1

if __name__ == "__main__":

    log = logging.getLogger('logger_weibo_reptile')
    main()


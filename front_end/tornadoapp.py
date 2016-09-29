#! /usr/bin/env python

from tornado.wsgi import WSGIContainer
from tornado.ioloop import IOLoop
from tornado.web import FallbackHandler, RequestHandler, Application
from app import app
from tornado import autoreload, websocket, web
import json, uuid, tornado, redis
import threading, time

global external_storage
external_storage = {}

def callback():
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    global redis_pub
    redis_pub = redis_client.pubsub()
    redis_pub.subscribe("bid_results")
    print "reading redis"

    for item in redis_pub.listen():
	    
	    try:
                val = json.loads(item["data"])
                print val
		external_storage[val["user_id"]].write_message(val["p_id"])
	    
	    except Exception as e:
		print e
		pass


class WebSocketHandler(tornado.websocket.WebSocketHandler):


    def check_origin(self, origin):
        return True

    def open(self):
        self.id = uuid.uuid4()
        print 'new connection'
        self.write_message("hello")
								
    def on_message(self, message):
        #Some message parsing here
        j = json.loads(message)

        if j["type"] == 'set_user_id':
           external_storage[j["user_id"]] = self
           print 'message received %s' % message

    def on_close(self):
        if len(external_storage) > 0 :
                external_storage[self.id] = ""
        print 'closed connection'


class MainHandler(RequestHandler):
 def get(self):
   self.write("This message comes from Tornado ^_^")

tr = WSGIContainer(app)

application = Application([
(r"/tornado", MainHandler),
(r'/websocket', WebSocketHandler),
(r".*", FallbackHandler, dict(fallback=tr)),
], debug=True)

if __name__ == "__main__":
 threading.Thread(target=callback).start()
 application.listen(80)
 ioloop = IOLoop.instance()
 autoreload.start(ioloop)
 ioloop.start()

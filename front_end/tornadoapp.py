#! /usr/bin/env python

from tornado.wsgi import WSGIContainer
from tornado.ioloop import IOLoop
from tornado.web import FallbackHandler, RequestHandler, Application
from app import app
from tornado import autoreload, websocket, web
import json, uuid, tornado
from kafka import KafkaConsumer

class WebSocketHandler(tornado.websocket.WebSocketHandler):
    
    global external_storage
    external_storage = {}
    #consumer = KafkaConsumer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    #consumer.subscribe(['my-topic'])

    def check_origin(self, origin):
        return True

    def open(self):
        self.id = uuid.uuid4()
        external_storage[self.id] = {'id':self.id}
        print 'new connection'
        self.write_message("Hello World")
  
    def on_message(self, message):
        #Some message parsing here
	j = json.loads(message)

        if j["type"] == 'set_user_id':
           external_storage[self.id]['user_id'] = j["user_id"]
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
 application.listen(80)
 ioloop = IOLoop.instance()
 autoreload.start(ioloop)
 ioloop.start()

from app import app
from flask import render_template
from flask import jsonify
from elastic_search_wrapper.es_processor import ElasticProcessor
import json
from kafka import KafkaProducer
import tornado.web
import tornado.httpserver

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

@app.route('/')
@app.route('/index')
def index():
  user = { 'nickname': 'Miguel' } # fake user
  return render_template("index.html", title = 'Home', user = user)

@app.route('/api/list/<lat>/<long>')
def get_lots(lat, long):
  
  dist_query = {
   "query":{
      "filtered":{
         "filter":{
            "and":{
               "filters":[
                  {
                     "range":{
                        "occ":{
                           "gte":0
                        }
                     }
                  },
                  {
                     "geo_distance":{
                        "distance":"2mi",
                        "location":{
                           "lat": lat,
                           "lon": long
                        }
                     }
                  }
               ]
            }
         }
      }
   }
  }
  #print dist_query
  usr_list = []
  usr_list.append({"lat":  lat, "lon": long})
  ew = ElasticProcessor()
  try:
     jsonresponse = ew.search_document_multi(usr_list)
     #ew.search_document(dist_query)
     #print jsonresponse
  except Exception as e:
     print e
  #jsonresponse = [{"name": "Garage1", "lat": 37.76425207, "long": -122.4207729, "occ": "122", "oper": "200"}, {"name": "Garage2", "lat": 37.7832776731, "long": -122.405537559, "occ": "35", "oper": "130"}]
  return jsonify(jsonresponse)

@app.route('/api/bid/<bid>/<lat>/<lon>')
def bid(bid, lat, lon):

  bid_data = {"bid": {"uid": "Srivats",  "lat": lat, "amt": int(bid), "long": lon}}
  
  try:
  	producer.send('userbid_stream_topic', bid_data)
  except:
	e = sys.exc_info()[0]
	print e
  return jsonify({"success" : "ok"})

 

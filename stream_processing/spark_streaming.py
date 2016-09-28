import sys
import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from elastic_search_wrapper.es_processor import ElasticProcessor
import json
from kafka import KafkaProducer
from collections import defaultdict
import datetime
import redis

def raw_data_tojson(sensor_data):
  """ Parse input json stream """
  raw_sensor = sensor_data.map(lambda k: json.loads(k[1]))
  t = raw_sensor.map(lambda x: x[x.keys()[0]])
  return t
 

if __name__ == "__main__":

    #if len(sys.argv) != 3:
        #print("Usage: kafka_wordcount.py <zk> <topic>")
        #exit(-1)

    sc = SparkContext(appName="ParkingStreamingCompute")
    ssc = StreamingContext(sc, 20)  # 10-sec window 

    #zkQuorum, topic = sys.argv[1:]
    zkQuorum = "localhost::2181"
    topic = "parking_stream_topic"
    topic2 = "userbid_stream_topic"
    kafkaBrokers = {"metadata.broker.list": "ec2-52-43-77-237.us-west-2.compute.amazonaws.com:9092, ec2-52-33-204-92.us-west-2.compute.amazonaws.com:9092, ec2-52-32-46-207.us-west-2.compute.amazonaws.com:9092, ec2-54-68-155-188.us-west-2.compute.amazonaws.com:9092"}
    park_data = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)
    bid_data = KafkaUtils.createDirectStream(ssc, [topic2], kafkaBrokers)
    
    print "==== Start ===="

    parkRdd = raw_data_tojson(park_data)
    bidRdd = raw_data_tojson(bid_data)
    #bid_list = bidRdd.map(lambda x: (x["uid"], x["amt"]))
    #sorted_bid = bid_list.transform(lambda x: x.sortBy(lambda y: -y[1]))
    
    def process_lots(rdd):

	print "inside elastic search updates"        
 	ew = ElasticProcessor()

	try:

	   doc_list = []	
           for kv in rdd:
       		doc_list.append(kv)
	   
	   if(len(doc_list) > 0):
	   	print ew.update_document_multi(doc_list)

	except Exception as e:
	   print e
	   pass
      
    def process_bids(rdd):
	
	print "inside process bids"
	results = defaultdict(list)
	
        ew = ElasticProcessor()
        try:

           usr_list = []
	   user_id = []
           
           for kv in rdd:
		user_id.append((kv["uid"], kv["amt"]))
		usr_list.append({"lat":  kv["lat"],"lon": kv["long"]})

	   if(len(usr_list) > 0):
	        res = ew.search_document_multi(usr_list)
		i = 0
		responses = res['responses']

        	for response in responses:
            		try:
                		hits = response['hits']['hits']
				
                		if len(hits) != 0:
				    for h in hits:
					park_lot = h['_source']
					p_id = park_lot['p_id']
					occ = park_lot['occ']
					name = park_lot['name']
					results[(p_id, occ)].append(user_id[i])
				
				i += 1	

            		except KeyError:
                		print Exception(response)
           
	   return results.items()
	
	except Exception as e:
	   print Exception(e)
           pass

    def assign_lots(rdd):
	 print "inside assign lots"
	 redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
         
	 # assign users with parking spots
	 for k,v in rdd:
	     print k, v
	    
  	     
    parkRdd.foreachRDD(lambda rdd: rdd.foreachPartition(process_lots))
    lots_map = bidRdd.mapPartitions(process_bids)
    lots_map.foreachRDD(lambda rdd: rdd.foreachPartition(assign_lots))
 
    #sorted_usrs = lots_map.transform(lambda x: x.items().sortBy(lambda y: -y[1]))  
    #sorted_usrs.pprint()
    
    redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
    redis_client.flushdb()

    print "=== End ===="
    ssc.start() 
    ssc.awaitTermination()

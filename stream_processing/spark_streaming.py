import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
from elastic_search_wrapper.es_processor import ElasticProcessor
import json
from kafka import KafkaProducer

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
    bid_list = bidRdd.map(lambda x: (x["uid"], x["amt"]))
    sorted_bid = bid_list.transform(lambda x: x.sortBy(lambda y: -y[1]))
    sorted_bid.pprint()
    #parkRdd = park_obj.map(lambda x: {"p_id" : x["pid"], "occ" : x["occ"]})
    
    def process_lots(rdd):
        
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
	
        ew = ElasticProcessor()
        try:

           usr_list = []
	   user_id = []           
           for kv in rdd:
		user_id.append(kv["uid"])
		usr_list.append({"lat":  kv["lat"],"lon": kv["long"]})

	   #producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dum$
	   #bid_res = {"uid": kv["uid"], "p_id": obj.pID}
           #producer.send('my-_topic', bid_res)
	   results = []

	   if(len(usr_list) > 0):
	        res = ew.search_document_multi(usr_list)

		#print res
		i = 0
		responses = res['responses']
		#print len(responses)
        	for response in responses:
            		try:
                		hits = response['hits']['hits']
				park_list = []
				
                		if len(hits) != 0:
				    for h in hits:
					park_lot = h['_source']
					p_id = park_lot['p_id']
					occ = park_lot['occ']
					name = park_lot['name']
					park_list.append((user_id[i] , (p_id, occ, name)))

				results.append(park_list)
				i += 1	

            		except KeyError:
                		print Exception(response)
    
	   return results
	
	except Exception as e:
	   raise Exception(e)
           pass

    parkRdd.foreachRDD(lambda rdd: rdd.foreachPartition(process_lots))
    lots_dict = bidRdd.mapPartitions(process_bids)
    lots_dict.pprint()

    #s2 = s2.filter(lambda x : x[1] > 0)
    #combined_info = s1.join(s2)
    #combined_info.pprint()
    #room_rate_gen = combined_info.map(lambda x: ((x[0][0]), x[0][1])).groupByKey().\
	#mapValues(list)

    print "=== End ===="
    ssc.start()
    ssc.awaitTermination()

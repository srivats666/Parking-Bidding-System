import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext, Row
import json

def raw_data_tojson(sensor_data):
  """ Parse input json stream """
  raw_sensor = sensor_data.map(lambda k: json.loads(k[1]))
  #raw_sensor.pprint()
  t = raw_sensor.map(lambda x: x[x.keys()[0]])
  #t.pprint()
  return t
 

if __name__ == "__main__":

    #if len(sys.argv) != 3:
        #print("Usage: kafka_wordcount.py <zk> <topic>")
        #exit(-1)

    sc = SparkContext(appName="ParkingStreamingCompute")
    ssc = StreamingContext(sc, 2)  # 1-sec window 

    #zkQuorum, topic = sys.argv[1:]
    zkQuorum = "localhost::2181"
    topic = "parking_stream_topic"
    topic2 = "userbid_stream_topic"
    kafkaBrokers = {"metadata.broker.list": "ec2-52-43-77-237.us-west-2.compute.amazonaws.com:9092, ec2-52-33-204-92.us-west-2.compute.amazonaws.com:9092, ec2-52-32-46-207.us-west-2.compute.amazonaws.com:9092, ec2-54-68-155-188.us-west-2.compute.amazonaws.com:9092"}
    park_data = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)
    bid_data = KafkaUtils.createDirectStream(ssc, [topic2], kafkaBrokers)
    
    #park_data.pprint()    
    park_obj = raw_data_tojson(park_data)
    bid_obj = raw_data_tojson(bid_data)
    s1 = bid_obj.map(lambda x: (x["pid"], (x["uid"], x["amt"], x["occ_now"])))   
    s2 = park_obj.map(lambda x: (x["pid"], x["occ"]))

    s1.pprint()
    print "======== s1 ========"
    s2.pprint()
    print "========= s2 ========="
    #s2 = s2.filter(lambda x : x[1] > 0)
    combined_info = s1.join(s2)
    combined_info.pprint()
    #room_rate_gen = combined_info.map(lambda x: ((x[0][0]), x[0][1])).groupByKey().\
	#mapValues(list)
    #room_rate_gen.pprint()
    #s1.pprint()
    #gp.pprint()

    print "==== Start ===="
    #counts.pprint()
    print "=== End ===="
    ssc.start()
    ssc.awaitTermination()
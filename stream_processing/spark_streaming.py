import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import psycopg2
from pyspark.sql import SQLContext, Row

def raw_data_tojson(sensor_data):
  """ Parse input json stream """
  raw_sensor = sensor_data.map(lambda k: json.loads(k[1]))
  return(raw_sensor.map(lambda x: json.loads(x[x.keys()[0]])))
 

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>")
        exit(-1)

    sc = SparkContext(appName="ParkingStreamingCompute")
    ssc = StreamingContext(sc, 300)  # 1-sec window 

    zkQuorum, topic = sys.argv[1:]
    zkQuorum = "localhost::2181"
    topic = "parking_stream_topic"
    kafkaBrokers = {"metadata.broker.list": "ec2-52-43-77-237.us-west-2.compute.amazonaws.com:9092, ec2-52-33-204-92.us-west-2.compute.amazonaws.com:9092, ec2-52-32-46-207.us-west-2.compute.amazonaws.com:9092, ec2-54-68-155-188.us-west-2.compute.amazonaws.com:9092"}
    kvs = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)
    
    park_loc = raw_data_tojson(kvs)
    s1 = raw_loc.map(lambda x: ((x["parking"]["uid"], x["parking"]["pid"]) , x["parking"]["amt"]))   
    

    print "==== Start ===="
    #counts.pprint()
    print "=== End ===="
    ssc.start()
    ssc.awaitTermination()

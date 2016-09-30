import json
import threading, logging, time, requests, json
import random, os
from kafka import KafkaProducer

class Parking(object):

    def __init__(self):
        self.lat = ""
        self.long = ""
        self.pID = ""

    def __repr__(self):
      return str(self)

simplelist = []
def make_data():

    with open('../data/parking_data.json') as data_file:
        data = json.load(data_file)

        for lots in data["AVL"]:
		y = Parking();

                if 'OSPID' in lots:
			id = lots["OSPID"]
		elif 'BFID' in lots: 
			id = lots["BFID"]

		loclist = lots["LOC"].replace(' ','').split(',')
		y.pID = id
		y.lat = loclist[1]
		y.long = loclist[0]
		simplelist.append(y)

    #print [student.pID for student in simplelist]

class Producer(threading.Thread):
    daemon = True

    def run(self):
	producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_BROKERS'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
	count = 0
        while True:
            rd = random.randint(0, len(simplelist) -1)
	    obj = simplelist[rd]
	    occ = random.randint(10, 170)
  	    park_data = {"parking": {"p_id": obj.pID, "occ": occ}}
            print park_data
	    producer.send('parking_stream_topic', park_data)
	    count + =1

	    if count == 500:
            	time.sleep(1)
		count = 0

def main():
    make_data()
    threads = [
        Producer(),
    ]

    for t in threads:
        t.start()

    while True:
        time.sleep(3)

if __name__ == "__main__":
    logging.basicConfig(
                format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()




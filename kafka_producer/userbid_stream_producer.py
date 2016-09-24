import json
import threading, logging, time, requests, json
import random
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
		loclist = lots["LOC"].replace(' ','').split(',')
		y.pID = lots["NAME"]
		y.lat = loclist[1]
		y.long = loclist[0]
		simplelist.append(y)

class Producer(threading.Thread):
    daemon = True

    def run(self):
	producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        while True:
            rd = random.randint(0, len(simplelist) -1)
            user_id = random.randint(0, 2000)
	    amt = random.randint(1, 100)
	    occ = random.randint(10, 400)
	    obj = simplelist[rd]
	    bid_data = {"bid": {"uid": user_id,  "lat": obj.lat, "amt": amt, "long": obj.long}}
            producer.send('userbid_stream_topic', bid_data)
            print bid_data
            time.sleep(2)

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




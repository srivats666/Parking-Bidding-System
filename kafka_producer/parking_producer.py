#!/usr/bin/env python
import threading, logging, time, requests, json

from kafka import KafkaConsumer, KafkaProducer


class Producer(threading.Thread):
    daemon = True

    def run(self):

	producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        while True:
	    r = requests.get('http://api.sfpark.org/sfpark/rest/availabilityservice?response=json&pricing=yes&radius=6.14592')
       	    output = r.json()	    
       	    producer.send('parkData-topic', output)
	    time.sleep(1000)


def main():
    threads = [
        Producer(),
        #Consumer()
    ]

    for t in threads:
	t.start()

    while True:	
    	time.sleep(10)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()

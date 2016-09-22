import json, random, logging, uuid, csv, sys
from elastic_search_wrapper.es_processor import ElasticProcessor

def make_data():

    f = open("../data/cities.csv", 'r')
    csv_f = csv.reader(f)
  
    for line in csv_f:
	occ = random.randint(10, 400)
	oper = random.randint(occ, occ + 200)
	id = uuid.uuid4().hex
	name = line[2] + line[0]
        lat = line[3]
        lon = line[4]		
	ew = ElasticProcessor()

	doc0 = {
		"location":
		{ "lat": lat,
  	          "lon": lon },
		"occ": occ,
		"oper": oper,
		"name": name,
		"p_id": id
	}
	try:
		print ew.create_document(doc0)
        	print doc0
	except:
		e = sys.exc_info()[0]
	  	print "<p>Error: %s</p>" % e 
	
if __name__ == "__main__":
    logging.basicConfig(
		format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    make_data()

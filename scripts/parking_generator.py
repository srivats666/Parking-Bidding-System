import json, random, logging, uuid
from elastic_search_wrapper.es_processor import ElasticProcessor

def make_data():

    with open('../data/parking_data.json') as data_file:
        data = json.load(data_file)

        for lots in data["AVL"]:
			loclist = lots["LOC"].replace(' ','').split(',')
			
			if 'OCC' not in lots or lots["OCC"] == '0':
				occ = random.randint(10, 400)
			else:	
				occ = lots["OCC"]
			
			if 'OPER' not in lots or lots["OPER"] == '0':
				oper = random.randint(occ, occ + 200)
			else:	
				oper = lots["OPER"]
			
			if 'OSPID' in lots:
				id = lots["OSPID"]
			elif 'BFID' in lots: 
				id = lots["BFID"]
			else:
				id = uuid.uuid4().hex
				
			ew = ElasticProcessor()

			doc0 = {
				"location":
					{ "lat": loclist[1],
					  "lon": loclist[0] },
				"occ": occ,
				"oper": oper,
				"name": lots["NAME"],
				"p_id": id
			}

			#print ew.create_document(doc0)
                        print doc0
	
if __name__ == "__main__":
    logging.basicConfig(
		format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    make_data()

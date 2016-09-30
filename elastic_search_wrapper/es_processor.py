from elasticsearch import Elasticsearch, helpers as eshelpers
import pdb, json, os

class ElasticProcessor():
    def __init__(self, index="parkingdata", type="point"):
        self.index = index
        self.type = type
	self.filter_path=['hits.hits.*']
	es_dns = os.environ['ES_DNS']
        self.es = Elasticsearch(
            [{'host':es_dns}] 
        )

    def delete_index(self):
        "Deletes one or more indices"
        return self.es.indices.delete(index=[self.index])
    
    def create_parking_index(self):
        "Create an index for my parkingdata"
        config = {
            "mappings": {
                self.type :   {
                    "dynamic":"strict",
                    "properties" : {
                        "p_id"  : { "type" : "string" },
                        "name"  : { "type" : "string" },
                        "location" : { "type" : "geo_point" },
                        "occ"    : { "type" : "string" },
                        "oper": { "type" : "string" }
                    }
                }
            }
        }
        return self.es.indices.create(index=self.index, body=config)

    def create_document(self, doc):
        #print id
        return self.es.create(index=self.index, doc_type=self.type, body=doc, id=doc["p_id"])
    
    def create_document_multi(self, docs):
        """
        Bulk indexes multiple documents. 
        docs is a list of document objects.
        """
        def add_meta_fields(doc):
            return {
                "_index": self.index,
                "_type":  self.type,
                "_id"  : doc["p_id"],
                "_source": doc
            }
    
        docs = map(add_meta_fields, docs)
        return eshelpers.bulk(self.es, docs)

    def update_document_multi(self, docs):
        """
        Bulk indexes multiple documents.
        docs is a list of document objects.
        """
        def add_meta_fields(doc):
            return {
                "_index": self.index,
                "_type":  self.type,
                "_id"  : doc["p_id"],
                "doc": doc,
		"_op_type": "update"
            }

        docs = map(add_meta_fields, docs)
        return eshelpers.bulk(self.es, docs)

    def search_document_multi(self, docs):
        """
        Bulk indexes multiple documents.
        docs is a list of document objects.
        """
        #def add_meta_fields(doc):
            #return "{}\n" + json.dumps(doc) + "\n"
            #'{"query":{"filtered": {"filter": {"and" :{"filters" : [{"range" : { "occ" : {"gte": 0} }}, {"geo_distance": {"distance": "1mi", "location": {"lat": "'+str(doc["lat"])+'", "lon": "'+str(doc["lon"])+'"}}}]}}}}}'

        #docs = map(add_meta_fields, docs)
	
	body_elems = []
	for doc in docs:
	    body_elems.append('{}')
	    body_elems.append('{"query":{"filtered": {"filter": {"and" :{"filters" : [{"range" : { "occ" : {"gte": 0} }}, {"geo_distance": {"distance": "1mi", "location": {"lat": "'+str(doc["lat"])+'", "lon": "'+str(doc["lon"])+'"}}}]}}}}}')
			
	body = '\n'.join(body_elems)
        res = self.es.msearch(index=self.index, doc_type=self.type, body=body)
	#print len(res['responses'])
	return res


    def get_mapping(self):
        return self.es.indices.get_mapping(index=self.index, doc_type=self.type)
    
    def search_document(self, query):
        return self.es.search(index=self.index, doc_type=self.type, body=query)
    
    def get_all(self):
        query = {"query" : {"match_all" : {}}}
        return self.es.search(index=self.index, doc_type=self.type, body=query)

if __name__ == "__main__":
    ew = ElasticProcessor()
    print os.environ['ES_DNS']
    ew.create_parking_index()

    doc0 = {
        "location":
            { "lat": 40.713,
              "lon": -73.986 },
        "occ": "32",
        "oper": "42",
        "name": "SF parking space",
        "p_id": "126"
    }

    doc1 = {'oper' : 2, 'occ': 1, 'p_id': '76b1ab68a3a4408f8992d818888ec731', 'location': {'lat': '36.68569', 'lon': '-85.140677'}, 'name' : 'Anytime Parking'}
    doc2 = {'p_id' : '76b1ab68a3a4408f8992d818888ec731', 'occ': 100}
    doc3 = {'occ': 42, 'p_id': u'568191'}
    #print ew.create_document(doc1)

    dist_query = {
      "query": {
        "filtered": {
          "filter": {
            "geo_distance": {
              "distance": "4mi",
              "location": {
                "lat":  36.68569,
                "lon": -85.140677
              }
            }
          }
        }
      }
    }
    
    dist_query4 = {"lat": 37.7900040233, "lon": -122.3907533695}
    dist_query5 = {"lat": 37.7781271374, "lon": -122.4201850734}
    dist_query6 = {"lat": 37.7849547433, "lon": -122.4328661663}
    dist_query7 = {"lat": 37.7804646952, "lon": -122.3905582378}
    
    dist_query1 = {"lat":  61.68569, "lon": -149.140677}
    dist_query2 = {"lat":  42.68569,"lon": -110.140677}
    dist_query3 = {"lat":  37.78352702274404, "lon": -122.44734831201174}

    #print ew.search_document_multi([dist_query1, dist_query2, dist_query3])
    #print ew.update_document_multi([doc0, doc2, doc3])
    #print ew.create_document_multi([doc0, doc2])
    #print ew.get_mapping()
    #print ew.search_document(dist_query)
    #print ew.delete_index()
    #pprint(ew.get_mapping())

    #print ew.get_all()

#   print ew.delete_index()
#   print ew.create_geo_index()

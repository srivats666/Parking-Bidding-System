from elasticsearch import Elasticsearch, helpers as eshelpers
import pdb

class ElasticProcessor():
    def __init__(self, index="parkingdata", type="point"):
        self.index = index
        self.type = type
        self.es = Elasticsearch(
            [{'host':'localhost'}] 
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

    def get_mapping(self):
        return self.es.indices.get_mapping(index=self.index, doc_type=self.type)
    
    def search_document(self, query):
        return self.es.search(index=self.index, doc_type=self.type, body=query)
    
    def get_all(self):
        query = {"query" : {"match_all" : {}}}
        return self.es.search(index=self.index, doc_type=self.type, body=query)

if __name__ == "__main__":
    ew = ElasticProcessor()
    #ew.create_parking_index()

    doc0 = {
        "location":
            { "lat": 40.713,
              "lon": -73.986 },
        "occ": "32",
        "oper": "42",
        "name": "test",
        "p_id": "126"
    }

    doc1 = {'oper': 243, 'occ': 122, 'p_id': '76b1ab68a3a4408f8992d818888ec731', 'location': {'lat': '36.68569', 'lon': '-85.140677'}, 'name': 'Albany_42602'}
    #print ew.create_document(doc1)

    dist_query = {
      "query": {
        "filtered": {
          "filter": {
            "geo_distance": {
              "distance": "2mi",
              "location": {
                "lat":  37.797364,
                "lon": -122.468291
              }
            }
          }
        }
      }
    }

    #create_document_multi(es, [doc0, doc1])
    #print ew.get_mapping()
    #print ew.search_document(dist_query)
    #print ew.delete_index()
    #pprint(ew.get_mapping())

    print ew.get_all()

#   print ew.delete_index()
#   print ew.create_geo_index()

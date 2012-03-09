import pymongo as pym
import re
class GEOvector:
    global_storage = {}#a cheap workaround to speed up processing
    def __init__(self, geo_id, word_map, mongo_conn, scale=1, pseudocount=0, source=None, auto_build=True, stopwords=None):
        self.stopwords = {}
        if stopwords is not None:
            s = open(stopwords, 'r')
            for line in s:
                self.stopwords[line.strip()] = 1
        self.mongo_conn = mongo_conn
        self.geo_id = geo_id
        self.word_map = word_map
        self.scale = scale
        self.pseudocount = pseudocount
        self.index = 0
        self.numWords = 0
        if auto_build:
            self._vec = self._buildVector(source = source)
        else:
            self._vec = None

    def buildVector(self, source=None):
        self._vec = self._buildVector(source)

    def _buildVector(self, source=None):
        if self.geo_id not in GEOvector.global_storage:
            try:
                word_dict = self.word_map
                connection = self.mongo_conn
                ps = self.pseudocount
                myvec = map(lambda x: ps, self.word_map.keys())
                query = {'geo_id': self.geo_id}
                if source is not None:
                    query = {'$and': [query, {'$or': [{'source':s } for s in source]} ]}
                sum_counts = 0        
                for x in connection.geo.word2geo.find(query):
                    if x['word'] not in self.stopwords:
                        if 'count' in x:
                            myvec[word_dict[x['word']]] += self.scale*x['count']
                            sum_counts += x['count']
                            self.numWords += x['count']
                        else:
                            myvec[word_dict[x['word']]] += self.scale
                            sum_counts += self.scale 
                            self.numWords += 1 
                
            except:
                import sys
                print sys.exc_info()
                return None
            GEOvector.global_storage[self.geo_id] = (myvec, self.numWords)
        else:
            self.numWords = GEOvector.global_storage[self.geo_id][0]
            return GEOvector.global_storage[self.geo_id][0]
        return myvec

   

    def getSources(self):
        try:
            connection = self._conn

            query = {'geo_id': self.geo_id}
            return [s for s in connection.geo.word2geo.find(query).distinct('source')]
                
        except:
            import sys
            print sys.exc_info()
            return None

    def __iter__(self):
        return self

    def next(self):
        if self._vec is None:
            raise StopIteration
        elif self.index == len(self._vec):
            raise StopIteration
        else:
            i = self.index
            self.index += 1
            return self._vec[i]

    def __getitem__(self, k):
        return self._vec[k]
    
   
    def __len__(self):
        if self._vec is None:
            return 0
        else:
            return len(self._vec) 
    


if __name__ == "__main__":
    connection = pym.Connection('localhost', 27017)
    collection = connection.geo.word2geo
    word_dict = {}
    words = collection.distinct("word")
    for i, word in enumerate(words):
        word_dict[word] = i


    gv = GEOvector('GSE2395',word_dict, mongo_conn=connection)
    print len(gv)
    """
    for x in gv:
        print x"""
                

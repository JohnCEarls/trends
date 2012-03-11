import pymongo as pym
import re
import bisect
class GEOvector:
    global_storage = {}#a cheap workaround to speed up processing
    def __init__(self, geo_id, word_map, mongo_conn, scale=1, pseudocount=0, source=None, auto_build=True, stopwords=None, cache=True):
        self.cache = cache
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
        self.curr = 0
        self.data = []
        self.col = []
        if auto_build:
            self.data, self.col = self._buildVector(source = source)

    def buildVector(self, source=None):
        self.data, self.col = self._buildVector(source = source)

    def getSparse(self):
        """
        returns a sparse representation of the geovector
        """
        return (self.data, self.col)

    def _buildVector(self, source=None):
        data = []
        col = []
        if self.geo_id not in GEOvector.global_storage or not self.cache:
            #try:
            word_dict = self.word_map
            connection = self.mongo_conn
            pseudocount = self.pseudocount
            query = {'geo_id': self.geo_id}
            
            if source is not None:
                query = {'$and': [query, {'$or': [{'source':s } for s in source]} ]}
            sum_counts = 0       
            mydict = {} 
            #accumulate counts
            for x in connection.geo.word2geo.find(query):
                if x['word'] not in self.stopwords:
                    w_i = word_dict[x['word']]
                    if 'count' in x:
                        mydict[w_i] = mydict.setdefault(w_i,pseudocount) + self.scale*x['count']
                        self.numWords += x['count']
                    else:
                        mydict[w_i] = mydict.setdefault(w_i,pseudocount) + self.scale
                        self.numWords += 1 
            #put counts in sparse maps
            for c, d in sorted([(w_i, v) for w_i, v in mydict.iteritems()]):
                data.append(d)
                col.append(c)
            """    
            except:
                import sys
                print sys.exc_info()"""
            if self.cache:
                GEOvector.global_storage[self.geo_id] = (data, col)
                
        else:#use global_storage
            self.numWords = len(GEOvector.global_storage[self.geo_id][0])
            return GEOvector.global_storage[self.geo_id]
        return (data, col) 

   

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
        if self.index == len(self.word_map):
            self.index = self.curr = 0
            raise StopIteration
        elif self.curr == len(self.data):#out of data
            self.index += 1
            return self.pseudocount
        else:
            val = self.pseudocount
            if self.col[self.curr] == self.index:
                val = self.data[self.curr]
                self.curr += 1
            self.index += 1
            return val

    def __getitem__(self, k):
        i = bisect.bisect_left(self.col, k)
        if k>=len(self.word_map):
            raise IndexError("list index out of range")
            
        if i!= len(self.data) and self.col[i] == k:
            return self.data[i]
        else:
            return self.pseudocount
    
   
    def __len__(self):
        return len(self.word_map)

    @classmethod
    def writeGlobal(cls, filename):
        import json
        f = open(filename, 'w')
        json.dump(cls.global_storage, f)
        f.close()
    @classmethod
    def loadGlobal(cls, filename):
        import json
        f = open(filename, 'r')
        cls.global_storage = json.load(f)
        f.close()
        

if __name__ == "__main__":

    connection = pym.Connection('localhost', 27017)
    
    collection = connection.geo.word2geo
    word_dict = {}
    words = collection.distinct("word")
    for i, word in enumerate(words):
        word_dict[word] = i
    GEOvector.loadGlobal("gv_test.json" )     

    gv = GEOvector('GSE2395',word_dict, mongo_conn=connection)

    print gv.getSparse()
    print "1"
    print sum(gv)
    print sum(gv)
    print len(gv)
    """
    for x in gv:
        print x"""
                

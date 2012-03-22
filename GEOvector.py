import pymongo as pym
import re
import bisect
class GEOvector:
    """
    Class to present a vector of word counts for a given geo id
    """
    global_storage = {}#a cheap workaround to speed up processing
    def __init__(self, geo_id, word_map, mongo_conn, scale=1, pseudocount=0, source=None, auto_build=True, stopwords=None, cache=True):
        """
        geo_id (required) - string containing the GEO id for which the vector is to be built
        word_map (required) - dictionary that defines the order for the vector. {'someword(string)':position(int)}
        mong_conn (required): pymongo connection object
        scale: (dep)
        pseudocount: default value all words present (also any word with a count is added to the pseudocount as a baseline)
        source: (dep)
        stopwords: string containing the filename of a text file containing words that are to be ignored
        cache: boolean True: use globalstorage if available and store in global storage if not, False: ignore global_storage
        auto_build: boolean True: build vector initially, False: do not build vector (you must call buildVector directly)
        
        """
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
        #self.index = 0#the iterators location when iterating
        self.numWords = 0
        #self.curr = 0#points to the current element in the sparse representation
        self.data = []
        self.col = []
        if auto_build:
            self.data, self.col = self._buildVector(source = source)

    def buildVector(self, source=None):
        """
        Build the vector
        source: list of strings - contains the names of sources of the text to use in building the geovector(abstract, geo_db, etc.)
        """
        self.data, self.col = self._buildVector(source = source)

    def getSparse(self):
        """
        returns a sparse representation of the geovector

        (data list, col_list)  col_list maps the data to the word location per the word_dict, i.e. data[i] is the word count of the word at position col[i]
        they are ordered by col.
        """
        return (self.data, self.col)

    def _buildVector(self, source=None):
        """
        Actually builds the vector
        """
        data = []
        col = []
        if self.geo_id not in GEOvector.global_storage or not self.cache:
            try:
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
                
            except:
                import sys
                print sys.exc_info()
            if self.cache:
                GEOvector.global_storage[self.geo_id] = (data, col)
                
        else:#use global_storage
            self.numWords = len(GEOvector.global_storage[self.geo_id][0])
            return GEOvector.global_storage[self.geo_id]
        return (data, col) 

   

    def getSources(self):
        """
        Returns a list of sources of text for this geo_id
        """
        try:
            connection = self._conn

            query = {'geo_id': self.geo_id}
            return [s for s in connection.geo.word2geo.find(query).distinct('source')]
                
        except:
            import sys
            print sys.exc_info()
            return None

    def __iter__(self):
        return self.next()

    def next(self):
        curr  = 0
        index = 0
        while (index < len(self.word_map)):
            if curr == len(self.data):#out of data
                index += 1
                yield self.pseudocount
            else:
                val = self.pseudocount
                if self.col[curr] == index:
                    val = self.data[curr]
                    curr += 1
                index += 1
                yield val

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
        """
        Writes the global_storage variable to a file in a json format
        filename - string - the name of the file to write to
        """
        import json
        f = open(filename, 'w')
        json.dump(cls.global_storage, f)
        f.close()

    @classmethod
    def loadGlobal(cls, filename):
        """
        Loads a previously created global_storage file
        """
        import json
        f = open(filename, 'r')
        cls.global_storage = json.load(f)
        f.close()
 
if __name__ == "__main__":
    import json
    from pymongo import ReplicaSetConnection, ReadPreference
    connection = ReplicaSetConnection("clutch:27017",  replicaSet="geo")

    connection.read_preference = ReadPreference.SECONDARY
    

    f = open("words.json",'r')
    word_dict = json.loads(f.read())
    f.close()
    #GEOvector.loadGlobal("gv_test.json" )     

    gv = GEOvector('GSE2395',word_dict, mongo_conn=connection, cache=False)

    print gv.getSparse()
    print "1"
    print sum(gv)
    print sum(gv)
    sadfjkl = raw_input('What is your name? ')
    for x in gv:
        if 
        print "inner: ",
        print x
        print "outer: ",
        for y in gv:
            if y>0:
                print str(y),
        print "return inner"
                

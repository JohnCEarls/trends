from Learner import Learner
import pymongo
from pymongo import Connection
import random
from GEOvector import GEOvector


def clean(geo_ids, thresh = 20):
    l=Learner()
    c = Connection('clutch')
    cleaned = []
    for x in geo_ids:
        if len([Y for Y in c.geo.word2geo.find({'geo_id':x}).limit(10)]) > 5:
            #if GEOvector( x, l.dictionary, c).numWords > thresh:
            cleaned.append(x)
    return cleaned

def buildParams(set1, set2, seeds = 4):
    """
    build 2 sets and null
    """ 
    f = open('cleaned_samples.json', 'r')
    mystr = f.read()
    f.close()
    grouped_samples = json.loads(mystr)
    merge = []
    for v in grouped_samples.itervalues():
        for vi in v:
            merge.append(vi)
    random.shuffle(merge)
    i = 0
    size = len(merge)/len(grouped_samples.keys())
    for k in grouped_samples.keys():
        grouped_samples[k] = merge[i:i+size]
        i += size 
    
    #class_map=[(i,k) for i, k in enumerate(grouped_samples.keys())]
    class_map = []
    for k in grouped_samples.keys():
        if k == set1:
            class_map.append((0,k))
        elif k == set2:
            class_map.append((1,k))
        else:
            class_map.append((2,k))

    labeled = []
    class_vec = []
    unlabeled = []
        
    for i,k in class_map:
        if seeds >= len(grouped_samples[k])-1:
            seeds =  3*(len(grouped_samples[k])/4)
        chosen = random.sample(grouped_samples[k], seeds)
        for u in grouped_samples[k]:
            if u not in chosen:
                unlabeled.append(u)
            else:
                labeled.append(u)
                class_vec.append(i)
    s_map = {}
    for k,v in grouped_samples.iteritems():
        for s in v:
            s_map[s] = [i for i, key in class_map if key == k][0]
    return (labeled, class_vec, unlabeled, s_map, class_map)
    
if __name__ == "__main__":
    import json
    import random
    c = Connection('clutch')
    phenos = [u'squamous cell carcinoma', u'asthma', u'normal', u'large cell lung carcinoma', u'adenocarcinoma', u'chronic obstructive pulmonary disease']
    random.shuffle(phenos)
    """
    grouped_samples = {}
    for x in c.geo.samples.distinct("phenotype"):
        for y in c.geo.samples.find({'phenotype': x}):
            if x not in grouped_samples:
                grouped_samples[x] = []
            grouped_samples[x].append(y['geo_accession'])

    for k,v in grouped_samples.iteritems():
        print k,
        grouped_samples[k] = clean(v)
        print len(grouped_samples[k])
    print "+"*20
    print json.dumps(grouped_samples)
    print "+"*20
    """
    print "loading global storage"
    GEOvector.loadGlobal("vectors.json")    
    
    print "initializing learner"
    from itertools import combinations
    trip_early = 0
    s2 = 'Nonei'
    for illll in range(10):
        for s1 , s2 in combinations(phenos, 2):
            trip_early += 1
            for seeds in [10]:#range(4, 30,3):
                for fp in [True]:#, False]:
                    for burn in [.1]:#, .5, 1.0]:
                        for alpha in [1.0]:#[.1, .5, 1.0, 2.0, 5.0]:
                            learner = Learner(nclasses=3, words_file="words.json", fit_prior=fp,burn=burn, alpha=alpha )
                            #print "building params"
                            labeled, class_vec, unlabeled, s_map, class_map = buildParams(s1, s2, seeds=seeds)
                            b = learner.EM(labeled, class_vec, unlabeled, max_iter=100)
                            correct = 0
                            incorrect = 0
                            correct_map = {}
                            incorrect_map = {}
                            for k, c in s_map.iteritems():
                                if c not in correct_map:
                                    incorrect_map[c] = {}
                                    for i in range(3):
                                        incorrect_map[c][i] = 0
                                    correct_map[c] = 0
                                mypred = learner.predict(k)
                                if mypred == c:
                                    correct += 1
                                    correct_map[c] += 1 
                                    """
                                    if c < 1:
                                        print str(c) + " : " + k"""
                                else:
                                   incorrect_map[c][mypred] += 1
                                   incorrect += 1
                            print "*"*10
                            print s1 + ' vs ' + s2
                            print "alpha: " + str(alpha)
                            print "burn: " +str(burn)
                            print "seeds:  " +str(seeds)
                            print "fit prior:" + str(fp)
                            print "accuracy:" + str(float(correct)/(correct + incorrect))
                            print correct_map
                            print incorrect_map

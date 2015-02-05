import igraph
import time
import random
from igraph import *
import copy
import math
#from scipy.sparse import *
import time
import numpy as np
#from reachabilityMatrix import reachability
from igraph._igraph import OUT
from numpy.random import sample
import getopt,sys    
from datetime import datetime
from numpy import linalg as LA
from shortestpath import *


def genDistances(g, vertexIndex, tpStart, tpEnd):
    
    dirpath='/Users/biru/workspace/temporalDecay/output/'
    
    fileName=dirpath + 'seedsActTime.txt.bak'
    fiA = open(fileName, "r")
    seedsAct={}
    line=fiA.readline()
    while line:
        eles=line.split()
        if int(eles[0]) not in seedsAct:
            seedsAct[int(eles[0])] = {}
        else:
            seedsAct[int(eles[0])][int(eles[1])]=int(eles[2])
            
        line=fiA.readline()
    fiA.close()    
    
    dists={}
    topicInd=0
    for topic in seedsAct:
        topicInd+=1
        if topicInd >= tpStart and topicInd <= tpEnd:
            seedInd=0
            for seed in seedsAct[topic]:
                if seed not in dists:
                    seedInd+=1
                    print "dj: ", str(topicInd), ":", str(seedInd), "/", str(len(seedsAct[topic])), (time.strftime("%H:%M:%S"))
                    (distance, previous)=dijkstra(g, vertexIndex[str(seed)], None, 1)
                    dists[vertexIndex[str(seed)]]=distance
                    
    np.savez(dirpath + "distance", dists=dists)
        
    
def main(argv):
    optlist,args=getopt.getopt(argv,'')
    
    g = Graph.Read_Lgl("/Users/biru/workspace/Reachability/digg.lgl")
    N=g.vcount()
    E=g.ecount()
    
    vertexName={}
    vertexIndex={}
    for i in xrange(N):
        vertexName[i]=g.vs[i]["name"]
        vertexIndex[g.vs[i]["name"]]=i
        
    genDistances(g, vertexIndex, int(args[0]), int(args[1]))

if __name__ == '__main__':
    main(sys.argv[1:])    
        
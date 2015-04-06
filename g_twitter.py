#this function calculate the g(\alpha))

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
    
T=0.001 

# note: the i is the index of the node, not it's ID, seeds contains seeds' name
def SSS(seeds, g, N, inNeighbors, lw):
    P={}
    rSSS=np.zeros([1, N])
    for i in xrange(N):
        P[i]=0
    
    found=False
    round=0
    while found==False:
        #print round
        prev_P = dict(P)
        for i in xrange(N):
            if i in seeds:
                P[i]=1
            
            else:
                #neighbors = g.neighbors(vertexName[i], "in")
                neighbors = inNeighbors[i]
                #inNeighbors=g.neighbors(i,"in")
                pt=1
                #for n in neighbors:
                #    pt = pt*(1-P[n]*g.es[g.get_eid(n,i)]["weight"])
                for node in neighbors:
                    #P[i] = 1- (1-P[i])*(1-P[node]*g.es[g.get_eid(node,i)]["weight"])
                    #pt = pt*(1-P[node]*g.es[g.get_eid(node,i)]["weight"])
                    pt = pt*(1-P[node]*lw)
                    #pt = pt*(1-P[node]* lw)
                P[i] = 1-pt
        
        #print sum(P.values())
        #print P
        diff = math.sqrt(sum((P[k] - prev_P[k])**2 for k in P.keys()))
        if diff<T:
            found=True
            
        round+=1
    #print P
    for i in xrange(N):
        if i not in seeds:
            rSSS[0,i]=P[i]
            
    return rSSS
            



# all index in this function are index of the node, not its ID
def SP1M(seed, g, N, distances, lw):
    #AM = g.get_adjacency()
    rSP1M=np.zeros([1, N])
    dists=distances[seed]
    
    P={}
    for i in xrange(N):
        if i == seed:
            P[i]={}
            P[i][0]=1
        else:
            P[i]={}
            P[i][dists[i]]=0
            P[i][dists[i]+1]=0
        
    
    parents=set()
    parents=parents.union(seed)
    step=0
    while len(parents)>0:
        step+=1
        tmp=set()
        plus1=set()
        for p in parents:
            children = g.neighbors(p, "out")
            for c in children:
                if step in P[c]:
                    tmp.add(c)
                    if step-1 in P[c]:
                        plus1.add(c)
                        
                    P[c][step]=1-(1-P[c][step])*(1-lw*P[p][step-1])
        
        #for c in plus1:
        #    P[c][step]=1-(1-P[c][step-1])*(P[c][step])
         
        parents=tmp
        
    for i in xrange(N):
        if i != seed:
            for k in P[i]:
                rSP1M[0,i]=1-(1-rSP1M[0,i])*(1-P[i][k])

    return rSP1M

# read required information from files
def gPrepare(g, vertexIndex):
    
    dirpath='/Users/biru/workspace/temporalDecay/output/'
    
    
    if  (os.path.exists(dirpath + "gPrepare.npz")==True) or (os.path.exists(dirpath + "gPrepare.npz")==False) : # do not use npz file
#         ret = np.load(dirpath + "gPrepare.npz")
#         seedsAct = ret["seedsAct"]
#         P=ret["P"]
#     else:
        # nodeID, infect_prob
        fileName=dirpath + 'meanP.txt.bak'
        fiP = open(fileName, "r")
        P={}
        line=fiP.readline()
        while line:
            eles=line.split()
            P[int(eles[0])]=float(eles[1])
            line=fiP.readline()
        fiP.close()
            
        # story_id, seedID, timeDiff
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
    
        np.savez(dirpath + "gPrepare", P=P, seedsAct=seedsAct)
        
    
    
    if (os.path.exists(dirpath + "distance.npz")==False):
        dists={}
        topicInd=0
        for topic in seedsAct:
            topicInd+=1
            seedInd=0
            for seed in seedsAct[topic]:
                if seed not in dists:
                    seedInd+=1
                    print "dj: ", str(topicInd), ":", str(seedInd), "/", str(len(seedsAct[topic])), (time.strftime("%H:%M:%S"))
                    (distance, previous)=dijkstra(g, vertexIndex[str(seed)], None, 1)
                    dists[vertexIndex[str(seed)]]=distance
                    
        np.savez(dirpath + "distance", dists=dists)
    else:
        ret = np.load(dirpath + "distance.npz")
        dists = ret["dists"]

    return (seedsAct, P, dists)
    
    
def gFunc(alpha):
    lw = 0.02
    
    g = Graph.Read_Lgl("/Users/biru/workspace/Reachability/digg.lgl")
    N=g.vcount()
    E=g.ecount()
    
    vertexName={}
    vertexIndex={}
    for i in xrange(N):
        vertexName[i]=g.vs[i]["name"]
        vertexIndex[g.vs[i]["name"]]=i
    
    inNeighbors={}
    for i in xrange(N):
        inNeighbors[i] = g.neighbors(vertexName[i], "in") # inNeighbors stores the index of node of the graph
    
    (seedsAct, meanP, distances) = gPrepare(g, vertexIndex)
    
    prob={}
    topicInd=0
    for topic in seedsAct:
        topicInd+=1
        
        prob[topic]={}
        seedInd=0
        for seed in seedsAct[topic]:
            seedInd+=1
            print str(topicInd), ":", str(seedInd), "/", str(len(seedsAct[topic])), (time.strftime("%H:%M:%S"))
            t = seedsAct[topic][seed]
            #prob[topic][seed] = SSS([vertexIndex[str(seed)]], g, N, inNeighbors, lw*math.exp( - alpha * t))
            prob[topic][seed] = SP1M(vertexIndex[str(seed)], g, N, distances, lw*math.exp( - alpha * t))
            
    h={}
    ret=0
    for node in vertexName:
        h[node]=0
        counts=0
        for topic in seedsAct:
            if node not in seedsAct.keys():
                counts+=1
                for seed in seedsAct[topic]:
                    h[node] = 1-(1-h[node])*(1-prob[topic][seed][vertexIndex[node]])
        
    ret = LA.norm(np.array(meanP)-np.array(h))
    
    return ret


def main(argv):
    optlist,args=getopt.getopt(argv,'')
    alpha=1
    gFunc(alpha)
        
if __name__ == '__main__':
    main(sys.argv[1:])
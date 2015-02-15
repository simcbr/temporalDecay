import igraph
import time
import random
from igraph import *
import copy
import math
import time
import numpy as np
import scipy.misc as sc
from igraph._igraph import OUT
from numpy.random import sample
import getopt,sys    
import matplotlib
import matplotlib.pyplot as plt
from plotCascadesHist import histogram
from itertools import combinations

Zdict={}
def findZ(k):
    if k in Zdict:
        return Zdict[k]
    
    if k==0:
        ret=1
        
    else:
        ret=0
        for i in xrange(k):
            ret += sc.comb(k-1,i)* findZ(i) * (k-i) * findZ(k-i-1)
    
    if k not in Zdict:
        Zdict[k]=ret 
    return ret


def iterateK(N,K):
    g= Graph.Full(N)
    
    treesNum=0
    ES=g.es()
    esIDs = range(len(ES))
    edgeSet=list(combinations(esIDs,K))
    for s in edgeSet:
        gm=g.copy()
        removeList = list(set(esIDs) - set(s))
        gm.delete_edges(removeList)
        
        ret = gm.subcomponent(0, OUT)
        if len(ret)==K+1:
            treesNum+=1
            
    return treesNum

def main(argv):
    optlist,args=getopt.getopt(argv,'')
    
    N=int(args[0])
    K=int(args[1])
            
        
    print "Graph:", N, K, iterateK(N, K)

    print "findZ:", findZ(K)

if __name__=="__main__":
    main(sys.argv[1:])
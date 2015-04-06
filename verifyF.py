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

Fdict={}
# assign x edges among k nodes and other i external nodes, to make sure these k nodes can be reached from an external node.
# p  is the number of nodes (including k nodes) can be the target of edges
# q is the external nodes which can be the source of edges. 
def findF(k,p,q,x):
    
    #print k, p, q, x
    
    if k in Fdict:
        if p in Fdict[k]:
            if q in Fdict[k][p]:
                if x in Fdict[k][p][q]:
                    return Fdict[k][p][q][x]
    
    if q>0:
        if p==k:
            if p==0:
                ret=sc.comb(q-1,x)
            else:
                ret=0
                for l in xrange(min(p*q+1, x+1)):
                    ret+=findF(k, p, 0, x-l)*sc.comb(p*q,l)
        if p==k+1:
            ret=0
            for l in xrange(min(p*q+1, x+1)):
                if l==0:
                    ret+=findF(k, p, 0, x-l)
                elif l==1:
                    ret+=findF(k, p, 0, x-l)*sc.comb(q,1)
                else:
                    ret+=findF(k, p, 0, x-l)*sc.comb(q,1)*sc.comb((p-1)*q, l-1)
    else:
        
        if k==0:
            if p==0:
                if x==0:
                    ret=1
                elif x>0:
                    ret=0
            elif p==1:
                if q>0:
                    ret=sc.comb(q,x)
                elif x==0:
                    ret=1
                elif x>0:
                    ret=0
                else:
                    print "error"
        elif k==1:
            if p==1:
                if x==1:
                    ret=1
                elif x>1:
                    ret=sc.comb(q+1,x-1)
                elif x==0:
                    ret=0
                else:
                    print "error 2"
            elif p==2:
                if x>=1:
                    ret=sc.comb(q*p+1, x-1)
                else:
                    ret=0
            else:
                print "error 3"
            
        else:
            ret=0
            for i in xrange(k):
                tmp=0
                for j in xrange(k-i-1,x-i):
                    tmp += findF(k-i-1,k-i,i+1,j)*(k-i)*findF(i, i, k-i, x-j-1)
                ret += tmp*sc.comb(k-1,i)
    
    if k not in Fdict:
        Fdict[k]={}
    if p not in Fdict[k]:
        Fdict[k][p]={}
    if q not in Fdict[k][p]:
        Fdict[k][p][q]={}
    Fdict[k][p][q][x]=ret
     
    
    return ret


def iterateK(N,K,X):
    g= Graph.Full(N, directed=True)
    
    treesNum=0
    ES=g.es()
    esIDs = range(len(ES))
    edgeSet=list(combinations(esIDs,X))
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
    X=int(args[2])

    
    print findF(1,2,1,5)
    #print "Graph:", N, K, iterateK(N, K, X)

    #print "findZ:", findF(K, K, 0, X)

if __name__=="__main__":
    main(sys.argv[1:])
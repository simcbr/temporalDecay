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
LEFT=0
RIGHT=1

# The basic idea is using dynamic programming, such that we try to separate these K nodes and root into two groups:  G1 and G2.
# G1 contains the root and K-i-1 nodes,  G2 contains i+1 nodes where one node is the "root" of G2.  Denote node v as a reference node which is the root of G2.
# 
# The difficult part is, there exists the edge between G1 and G2; and we should PREVENT duplicated counting the same case of having edge from v to a node u TWICE 
#     1) u is in G1  2) u is in G2.
# 
# To solve the problem, define a function S(), such that  S(k, k+1, q, x, side) defines the total number of graphs we can have by having x edges end in k+1 nodes 
# (plus the root of these k nodes),  q is the number of external nodes;  x is the number of edges,  where the root can reach all k nodes.
# "side" is to specify G1 and G2:  LEFT for G1 case; RIGHT for G2 case; and the external nodes are on the opposite side of these k nodes (the reason to define RIGHT 
# and LEFT side is to prevent duplicated case, and we will handle LEFT and RIGHT cases differently).
# 
# 
# The whole algorithm based on following two equations:
# 
# initial :        S(0,1,0,0)=1;  S(0,1,0,x)=0 for x>0
# 
# 1)  S(k, k+1, q, x, side)  = \sum_{i=0}^{k-1} {k-1 \choose i} * \sum_{k-i-1, x-i-1} S(k-i-1,k-i,i+1,j, LEFT) * S(i, i+1, k-i, x-j, RIGHT)
# the \sum_{i=0}^{k-1} is to iterate all different size of G1 and G2;  {k-1 \choose i} is to include different combinations of nodes for specific sized G1 and G2.
# 
# 
# 2)  S(k, k+1, q, x, LEFT) =  \sum_{l=0}^{min((k+1)*q, x)}  S(k, k+1, 0, x-l, LEFT) * {(k+1)*q \choose l}   :
#        for G1 (it contains root), allocate x-l edges end within k nodes and the root, and left l edges originates from q external nodes and end at these k+1 nodes.    
#     S(k, k+1, q, x, RIGHT) = \sum_{l=1}^{min((k+1)*q, x)}  S(k, k+1, 0, x-l, LEFT) * { q \choose l}    :
#        allocate x-l edges end within k nodes and the reference node v of G2, and left l edges originates from q external nodes and end at node v.  
#        l is greater than or equal 1, since there must have one edge from G1 to G2's root v. This guarantees there are edged from G1 to v.

def findF(k,p,q,x, side):
    
    #print k, p, q, x
    
    if k in Fdict:
        if p in Fdict[k]:
            if q in Fdict[k][p]:
                if x in Fdict[k][p][q]:
                    if side in Fdict[k][p][q][x]:
                        return Fdict[k][p][q][x][side]
    
    if q>0:
        if side==LEFT: #left
            ret=0
            for l in xrange(min(p*q+1, x+1)):
                ret+=findF(k, p, 0, x-l, LEFT)*sc.comb(p*q,l)
            
        elif side==RIGHT: #right
            ret=0
            for l in xrange(min(p*q+1, x+1)):
                if l==0:
                    ret+=0 #findF(k, p, 0, x-l, LEFT)  it requires at least one edge from left to right.
                else:
                    
                    ret+=findF(k, p, 0, x-l, LEFT)*sc.comb(q,l) #external nodes can only connect the "root" of the right tree
                    
                    #ret+=findF(k, p, 0, x-l, LEFT)*sc.comb(q,1)*sc.comb((p-1)*q,l-1)   
    else:
        if k==0:
            if p==1:
                if q==0:
                    if x==0:
                        ret=1
                    else:
                        ret=0
                else:
                    print "Error 1"
            else:
                print "Errro 0"
        else:
            ret=0
            for i in xrange(k):
                tmp=0
                for j in xrange(k-i-1,x-i):
                    tmp += findF(k-i-1,k-i,i+1,j, LEFT)*findF(i, i+1, k-i, x-j, RIGHT)
                ret += tmp*sc.comb(k-1,i)
                
    
    if k not in Fdict:
        Fdict[k]={}
    if p not in Fdict[k]:
        Fdict[k][p]={}
    if q not in Fdict[k][p]:
        Fdict[k][p][q]={}
    if x not in Fdict[k][p][q]: 
        Fdict[k][p][q][x]={}
    Fdict[k][p][q][x][side]=ret
     
    
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

    
    #print findF(3,3,0,12,LEFT)
    #print findF(1,2,1,2,RIGHT)
    print "Graph:", N, K, iterateK(N, K, X)

    print "findZ:", findF(K, K, 0, X, LEFT)

if __name__=="__main__":
    main(sys.argv[1:])
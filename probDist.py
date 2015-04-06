
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
from verifyF2 import findF, LEFT, RIGHT
from json.decoder import linecol

Zdict={}
# this function is using dynamic programming to find, given a seed and k nodes, how many combinations to have k edges such that it is able connect these k nodes from the seed.
# it's based on:  
#  f(1)=1
# f(2)=f(1)*2 +f(1)
# f(3)=f(2)*3+f(2)
# ... 
# f(k)=f(k-1)*k + f(k-1) = f(k-1)*(k+1)
# given f(k-1), this means we know there are f(k-1) combinations to connect k-1 nodes from the seed.
# to connect k nodes, we first connect k-1 nodes, and we have f(k-1) combinations.
# for each combination, we can active the edge from the previous k-1 nodes plus the seed to this kth node. this gives f(k-1)*k different edge sets
#, then connect the seed to this kth node (no other sets contain the edge from the seed to kth node), to find ways to connect other k-1 nodes with k-1 edges, 
# it also have f(k-1) combinations. 
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
    
    
def cascadesDist(N, prob):
    E=N*(N-1)
    
    dirpath='/Users/biru/workspace/temporalDecay/output/'
    filename=dirpath + "cascadesDict-" + str(N) + "-" + str(prob)
    
    prob=1.0*prob/100
    
    dists={}
    
    y=int(prob*E)
            
    maxInfected=min(N-1,y)
    for k in range(1,maxInfected+1):
        
         
        x_max = min(k*(k+1),y)
                
        p=0
        h=(N-k-1)*(N-k-2) + (N-k-1)*(k+1)
                
        for x in range(k, x_max):
            t=1.0
            S=findF(k, k+1, 0, x, LEFT)
                    
            for j in range(1,y-x+1):
                t*=1.0*(h-j+1)/(E-j+1)
            for j in range(y-x+1,y+1):
                t*=1.0*j/(E-j+1)
            t*=S
                    
            p+=t
        p*=sc.comb(N-1, k)
                
        dists[k]=p
        
        print y, k, "/", maxInfected, p    
    
    
    np.savez(filename, dists=dists, y=y)
        
    
    
def plotCascadesDist(N, prob):
#So, given y=E*p active edges, name the group of k nodes as g1 (including the root), and left nodes as g2. the probability of a k-size cascade is

# x edges in G1 and root can only reach these k nodes.
# C(N-1,k) * \sum_{x=k}^{min(y, (k+1)*k)} S(k,x) * C(h, y-x) / C(E, y)
    E=N*(N-1)
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(24,6))
    
    fs = 80 # fontsize
    ts = 60 # tick axis font size
    
    
    dirpath='/Users/biru/workspace/temporalDecay/output/'
    ret = np.load(dirpath + "cascadesDict-" + str(N) + "-" + str(prob) + ".npz")
    dists = ret["dists"]
    dists = dists.item()
    y=ret["y"]
    
    #ax.plot(np.log(dists.keys()), np.log(dists.values()))
    ax.plot(dists.keys(), dists.values(), linewidth=6.0, color='k')
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xbound(0,100)
    ax.tick_params(axis='x', labelsize=ts)
    ax.tick_params(axis='y', labelsize=ts)
    #ax.set_xlabel('log #cascade size', fontsize=fs)
    #ax.set_ylabel('log Prob', fontsize=fs)
    ax.set_title("#edges=" + str(y), fontsize=fs)
                   
    #fig.tight_layout()
    fig.savefig('/Users/biru/workspace/temporalDecay/pic/' + str(N) + "-" + str(prob) + ".eps", format='eps', dpi=1000)

    plt.show()                
                

def main(argv):
    matplotlib.use('pgf')
    optlist,args=getopt.getopt(argv,'')
    
    cascadesDist(int(args[0]), int(args[1]))
    plotCascadesDist(int(args[0]), int(args[1]))
                
    
if __name__=="__main__":
    main(sys.argv[1:])

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



def version1(N):
#probability to have a size-i cascade: 
# \sum_{i=k}^{E-(N-k-1)*(k+1)}  [C(E,i)* p^i * (1-p)^(E-i)] * [\prod_{j=1}^{i} (N-j)*j/(E-j+1) *  C(E-k -(N-k-1)*(k+1), m-k)/C(E-k, m-k) ] 
    
    E=N*(N-1)
    fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(24,6))
    probs=[0.1,0.18,0.24]
    ind=0
    for ax in axes:
        prob=probs[ind]
        dist=[]
        
        for k in range(1,N):
            
            base=1.0
            for j in range(1,k+1):
                base*=1.0*(N-j)*j/(E-j+1)
            
            M=E-k-(N-k-1)*(k+1)
            
            p=0
            for i in range(k, M+k+1):
                t=0
                 
                for j in range(1,k+1):
                    t+=np.log(E-j+1) - np.log(k-j+1)
                         
                for j in range(k+1,i+1):
                    t+=np.log(M-j+k+1) - np.log(i-j+k+1)
                     
                t += i*np.log(prob) + (E-i)*np.log(1-prob)
                    #t += np.log(sc.comb(E-k-(N-k-1)*(k+1),i-k))
                     
                p+=np.exp(t)


            p*=base
            print k, i,"/",E-(N-k-1)*(k+1)+1,p,t,base
            
            dist.append(p)
            
        print dist
        ax.plot(np.log(range(1,N)), np.log(dist))
        ax.set_xlabel('#cascade size')
        ax.set_ylabel('Prob')
        ax.set_title("p=" + str(prob))
            
        ind+=1
        
    fig.tight_layout()
    #fig.savefig('/Users/biru/workspace/temporalDecay/pic/' + str(N) + "-" + str(args[0]) + ".eps", format='eps', dpi=1000)

    plt.show()    

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
    if k==1:
        return 1
    else:
        return findZ(k-1)*(k+1)


def version2(N):
#So, given y=E*p active edges, name the group of k nodes as g1 (it contains k(k+1) edges), and left nodes as g2. the probability of a k-size cascade is

# i edges in G1 (k*(k+1))
# Z * C(k*(k+1)-k, i-k) * C((N-k-1)*(N-k-2), y-i) / C(E, y)
    
    E=N*(N-1)
    fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(24,6))
    probs=[2.0/12,3.0/12,4.0/12]
    ind=0
    for ax in axes:
        prob=probs[ind]
        dist=[]
        
        y=int(E*prob)
        maxInfected=min(N,y)
        for k in range(1,maxInfected+1):
            #k=maxInfected-1
            x = min(k*(k+1),y)
            
#             Z=1.0
#             for j in range(1,k+1):
#                 Z*=((k-j+1)*j)
            Z=findZ(k)
            
            p=0
            h=(N-k-1)*(N-k-2) + (N-k-1)*(k+1)
            
            for i in range(k, x+1):
                t=1.0
                
                for j in range(1,i-k+1):
                    t*=1.0*(k*k-j+1)/(j)
                for j in range(y-i+1,y+1):
                    t*=1.0*j/(E-j+1)
                for j in range(1,y-i+1):
                    t*=1.0*(h-j+1)/(E-j+1)
                
                p+=t
            p*=Z
            p*=sc.comb(N-1,k)
            dist.append(p)
        
        print dist
        #dist = [x/sum(dist) for x in dist]
        
        #ax.plot(np.log(range(1,maxInfected+1)), np.log(dist))
        ax.plot(range(1,maxInfected+1), dist)
        ax.set_xlabel('#cascade size')
        ax.set_ylabel('Prob')
        ax.set_title("p=" + str(prob))
            
        ind+=1
        
    fig.tight_layout()
    #fig.savefig('/Users/biru/workspace/temporalDecay/pic/' + str(N) + "-" + str(args[0]) + ".eps", format='eps', dpi=1000)

    plt.show()            

def main(argv):
    matplotlib.use('pgf')
    optlist,args=getopt.getopt(argv,'')
    
    version2(50)
                
                
    
if __name__=="__main__":
    main(sys.argv[1:])
# this file find the \bar{P} and S for each story
import math      
import string
import random    
from diggSqlCon import DIGGSQLCONN
from datetime import datetime
import os
import getopt,sys
import numpy as np
from random import randrange


def dictMean(d):
    
    value=0
    count=0
    for k in d.keys():
        value += k*d[k]
        count += d[k]
    
    return 1.0*value/count

class PREPARE:
    
    def __init__(self):
        #self.v_dirpath='D:\workspace\Data\Network data\citation network\Terms map\\'
        self.v_dirpath='/Users/biru/workspace/temporalDecay/output/'
        #self.v_dirpath='D:/workspace/Data/Network data/Digg/'
        self.v_sql = DIGGSQLCONN()
        self.v_sql.openConn('localhost', 'root', 'cui', 'Digg')

    

    def seedsActTime(self):
        fileName=self.v_dirpath + 'seedsActTime.txt'
        foA = open(fileName, "r")
        
        fileName=self.v_dirpath + 'seedsActTimeMean.txt'
        foS = open(fileName, "w+")
        
        totalTimes=[]
        storyMeanTimes={}
        line=foA.readline()
        storyID=0
        while line:
            eles=line.split()
            id=int(eles[0])
            diff=int(eles[2])
            
            totalTimes.append(diff)
            
            if storyID==0:
                storyID=int(eles[0])
                times={}
                times[diff]=1
            elif storyID!=id:
                storyMeanTimes[storyID]=dictMean(times) 
                storyID=id
                times={}
                times[diff]=1
            else:
                if diff not in times:
                    times[diff]=1
                else:
                    times[diff]+=1
            
            line=foA.readline()
            
        for t in storyMeanTimes:
            foS.write(str(t))
            foS.write("\t")
            foS.write(str(storyMeanTimes[t]))
            foS.write("\n")
        
        foS.close()
        foA.close()
        
        print "total mean, min, max, std:", np.mean(totalTimes), np.min(totalTimes), np.max(totalTimes), np.std(totalTimes)
        
        

    def infectionTime(self):
        CNUM=self.v_sql.storiesNum()
        
        fileName=self.v_dirpath + 'storyInterval.txt'
        foS = open(fileName, "w+")
        fileName=self.v_dirpath + 'accumInterval.txt'
        foA = open(fileName, "w+")
        
        accumTimes={}
        for i in xrange(CNUM):
            # for each story
            print i,CNUM            
            # find all cascades of this story
            times = self.v_sql.extractAccuInfectionTime(i+1)
            for t in times:
                foS.write(str(i))
                foS.write("\t")
                foS.write(str(t))
                foS.write("\t")
                foS.write(str(times[t]))
                foS.write("\n")
                if t not in accumTimes:
                    accumTimes[t]=1
                else:
                    accumTimes[t]+=1
            
        for t in accumTimes:
            foA.write(str(t))
            foA.write("\t")
            foA.write(str(accumTimes[t]))
            foA.write("\n")
            
            
        foS.close()
        foA.close()
        print "accumTime mean, min, max, std: ", np.mean(accumTimes.values()), np.min(accumTimes.values()), np.max(accumTimes.values()), np.std(accumTimes.values()) 
        

    def infectionCascades(self):
        CNUM=self.v_sql.storiesNum()
        
        #stories={}
        PCR=[]
        for i in xrange(CNUM):
            # for each story
            print i,CNUM      
            C=[]
            # find all cascades of this story
            (seedsActTime, infected, cascades) = self.v_sql.extractInfection(i+1)
            for c in cascades:
                # bread first search
                #print cascades[c].treeDepth()
                cascades[c].BFS(C)
            PCR.append(C)
                
            #stories[i]=cascades
        
        np.savez("/Users/biru/workspace/temporalDecay/output/diggCascadesPCR", PCR=PCR)
        


    def infection(self):
        # each initial seed create a cascade (the cascade could be trivial: single node; or small: several nodes)
        CNUM=self.v_sql.storiesNum()
        P={}
        users = self.v_sql.allUsers()
        for user in users:
            P[user[0]]=[0,0]
            
        
        # output it
        fileName=self.v_dirpath + 'meanP.txt'
        foP = open(fileName, "w+")
        # nodeID, infect_prob
        
        fileName=self.v_dirpath + 'seedsActTime.txt'
        foA = open(fileName, "w+")
        # story_id, seedID, timeDiff
        
        fileName=self.v_dirpath + 'infectedBinV.txt'
        foI = open(fileName, "w+")
        # story_id, seedID, timeDiff
        
        fileName=self.v_dirpath + 'cascades.txt'
        foC = open(fileName, "w+")
                
        for i in xrange(CNUM):
            # for each story
            print i,CNUM            
            # find all cascades of this story
            (seedsActTime, infected, cascades) = self.v_sql.extractInfection(i+1)

            for node in cascades:
                foC.write(str(i+1))
                foC.write("\t")
                foC.write(str(len(cascades[node].treeSet())))
                foC.write("\t")
                foC.write(str(len(infected)))
                foC.write("\n")

            for seed in seedsActTime:
                P[seed][0] += 1
                foA.write(str(i+1))
                foA.write("\t")
                foA.write(str(seed))
                foA.write("\t")
                foA.write(str(seedsActTime[seed]))
                foA.write("\n")
            
            for node in infected:
                P[node][1] += 1
                foI.write(str(node)) # only record the newly infected nodes ID 
                foI.write("\t")
            foI.write("\n")
                
#             for node in P:
#                 if node in seedsActTime:
#                     foI.write("2\t") 
#                 elif node in infected:
#                     foI.write("1\t")
#                 else:
#                     foI.write("0\t")
#             foI.write("\n")
                
        for node in P:
            foP.write(str(node))
            foP.write("\t")
            foP.write(str(1.0*P[node][1]/(CNUM-P[node][0])))
            foP.write("\n")
            
            
        foA.close()
        foP.close()
        foI.close()
        foC.close()
        
def main(argv):
    optlist,args=getopt.getopt(argv,'')
    prepare = PREPARE()        
    #prepare.infection()
    #prepare.infectionTime()
    prepare.infectionCascades()
        
if __name__ == '__main__':
    main(sys.argv[1:])        
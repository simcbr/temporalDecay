# this file find the \bar{P} and S for each story
import math      
import string
import random    
from uscTwitterSqlCon import USCTWITTERSQLCON
from datetime import datetime
import os
import getopt,sys
from random import randrange

class PREPARE_TW:
    
    def __init__(self):
        #self.v_dirpath='D:\workspace\Data\Network data\citation network\Terms map\\'
        self.v_dirpath='/Users/biru/workspace/temporalDecay/output/'
        #self.v_dirpath='D:/workspace/Data/Network data/Digg/'
        self.v_sql = USCTWITTERSQLCON()
        self.v_sql.openConn('localhost', 'root', 'cui', 'Twitter')


    def infection(self):
        # each initial seed create a cascade (the cascade could be trivial: single node; or small: several nodes)
        URLs=self.v_sql.linkURLs()
        P={}
        users = self.v_sql.allUsers()
        for user in users:
            P[user[0]]=[0,0]
        
        # output it
        fileName=self.v_dirpath + 'meanP_tw.txt'
        foP = open(fileName, "w+")
        # nodeID, infect_prob
        
        fileName=self.v_dirpath + 'seedsActTime_tw.txt'
        foA = open(fileName, "w+")
        # story_id, seedID, timeDiff
        
        fileName=self.v_dirpath + 'infectedBinV_tw.txt'
        foI = open(fileName, "w+")
        # story_id, seedID, timeDiff
        
        fileName=self.v_dirpath + 'cascades_tw.txt'
        foC = open(fileName, "w+")
        
        linkInd=0 
        for url in URLs:
            # for each story
            linkInd +=1
              
            # find all cascades of this story
            (seedsActTime, infected, cascades) = self.v_sql.extractInfection(url[0])

            
            print linkInd,len(URLs), len(cascades), len(infected)
            for node in cascades:
                #print linkInd, len(cascades[node].treeSet()), len(infected)
                foC.write(str(linkInd))
                foC.write("\t")
                foC.write(str(len(cascades[node].treeSet())))
                foC.write("\t")
                foC.write(str(len(infected)))
                foC.write("\n")

            for seed in seedsActTime:
                P[seed][0] += 1
                foA.write(str(linkInd))
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
            foP.write(str(1.0*P[node][1]/(len(URLs)-P[node][0])))
            foP.write("\n")
            
            
        foA.close()
        foP.close()
        foI.close()
        foC.close()
        
def main(argv):
    optlist,args=getopt.getopt(argv,'')
    prepare_tw = PREPARE_TW()        
    prepare_tw.infection()
        
if __name__ == '__main__':
    main(sys.argv[1:])        
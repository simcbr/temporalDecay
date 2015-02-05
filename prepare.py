# this file find the \bar{P} and S for each story
import math      
import string
import random    
from diggSqlCon import DIGGSQLCONN
from datetime import datetime
import os
import getopt,sys
from random import randrange

class PREPARE:
    
    def __init__(self):
        #self.v_dirpath='D:\workspace\Data\Network data\citation network\Terms map\\'
        self.v_dirpath='/Users/biru/workspace/temporalDecay/output/'
        #self.v_dirpath='D:/workspace/Data/Network data/Digg/'
        self.v_sql = DIGGSQLCONN()
        self.v_sql.openConn('localhost', 'root', 'cui', 'Digg')


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
    prepare.infection()
        
if __name__ == '__main__':
    main(sys.argv[1:])        
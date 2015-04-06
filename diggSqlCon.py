import MySQLdb as mdb
import sys
import time
import math      
import random
import numpy as np
from tree import TREE

# SQL cmd
# create table author (id int not null auto_increment, author varchar(100) default null, paper_id int default null, primary key(id));
# alter table DBLP.term add index 'term'('term' ASC);


# The semantics of the friendship links are as follows 
# user_id --> friend_id 
# means that user_id is watching the activities of (is a fan of) friend_id. 


class DIGGSQLCONN:
    def __init__(self):
        self.v_con = None
        self.v_cur = None
        self.v_ind = 0
    
    def openConn(self, hostname, usrname, usrpwd, dbname):
        try:
            self.v_con = mdb.connect(hostname, usrname, usrpwd, dbname, local_infile=1)
            self.v_cur = self.v_con.cursor()
            #self.v_con=mysql.connector.connect(user=usrname, password=usrpwd, host=hostname, database=dbname)
            #self.v_cur = self.v_con.cursor(buffered=True, raw=True)
            #self.v_con.autocommit(True);
        except mdb.Error, e:
            print "Error %d: %s" % (e.args[0],e.args[1])
            sys.exit(1)      
            
    def commit(self):
        self.v_con.commit()        
                            
    def closeConn(self):
        if self.v_con:    
            self.v_con.close()   
            
    def outputQuery(self, queryStr, printEnable, num):
        selectstr = queryStr + " ;"
        self.v_cur.execute(selectstr)
        row = self.v_cur.fetchall()
        
        if printEnable==1:
            for i in range(len(row)):
                print row[i][0]
        else:
            ret=[]
            for i in range(len(row)):
                raw = []
                for j in range(num):
                    raw.append(row[i][j])
                ret.append(raw)
            return ret                    
        
        
            
    def usersNum(self): 
        qStr="select count(*) from users"
        ret=self.outputQuery(qStr,0,1)
        return ret[0][0]
    
    def allUsers(self):
        qStr="select distinct(user_id) from users"
        ret=self.outputQuery(qStr, 0, 1)
        return ret
        
    def edgesNum(self):
        qStr="select count(*) from friends"
        ret=self.outputQuery(qStr,0,1)
        return ret[0][0]
        
    def storiesNum(self):
        qStr="select count(distinct(story_id)) from votes"
        ret=self.outputQuery(qStr,0,1)
        return ret[0][0]
        
        
    def actProb(self, friend_id, user_id):
        quote="\""
        qStr="select prob_same_act from friends where user_id=" + quote + str(user_id) + quote + " and friend_id=" + quote + str(friend_id) + quote
        ret=self.outputQuery(qStr,0,1)
        return ret[0][0]
            
            
    def actProbs(self, friend_id):
        quote="\""
        qStr="select user_id, prob_same_act from friends where friend_id=" + quote + str(friend_id) + quote
        ret=self.outputQuery(qStr,0,2)
        return ret
            
            
    def netinfProb(self, friend_id, user_id):
        quote="\""
        qStr="select prob_netinf from friends where user_id=" + quote + str(user_id) + quote + " and friend_id=" + quote + str(friend_id) + quote
        ret=self.outputQuery(qStr,0,1)
        return ret[0][0]            
            
        
    def list2array(self, l):
        a=[]
        for i in range(len(l)):
            a.append(l[i][0])
        return a
        
    
    def arrayToDict(self, A):
        ret={}
        for t in A:
            if t[0] not in ret.keys():
                ret[t[0]]=t[1]
        return ret
    
    
               
        
        
        

        
    def extractAccuInfectionTime(self, story_id):
        quote="\""
        qStr = "select vote_time from connVotes where story_id=" + quote + str(story_id) + quote + " order by vote_time"
        ret=self.outputQuery(qStr,0,1)
        timelist = self.list2array(ret)
        qStr = "select min(vote_time) from connVotes where story_id=" + quote + str(story_id) + quote 
        ret=self.outputQuery(qStr,0,1)
        mintime = ret[0][0]
        
        times = {}
        for t in timelist:
            diff = self.hourDiff(self.convertUnixTime(mintime), self.convertUnixTime(t))
            if diff not in times.keys():
                times[diff] = 1
            else:
                times[diff] += 1
            
        return times
        
        
    def pickPartialGraph(self, Nodes):
        nodesStr=str(Nodes)
        nodesStr=nodesStr.replace('[', '(')
        nodesStr=nodesStr.replace(']', ')')
        nodesStr=nodesStr.replace('L', '')
            
        edges=[]
        quote="\""
        for node in Nodes:
            qStr = "select user_id from friends where friend_id=" + quote + str(node) + quote + " and user_id in " + nodesStr
            ret=self.outputQuery(qStr,0,1)
            targets = self.list2array(ret)
            for t in targets:
                edges.append((node, int(t)))
        
        return edges
        
    # the function extract seeds' activated time and which node is infected in this story
    # the time is relative compared to the start time of this story
    def extractInfection(self, story_id):
        cascades={}
        quote="\""
        qStr = "select vote_time, voter_id from connVotes where story_id=" + quote + str(story_id) + quote + " order by vote_time"
        ret=self.outputQuery(qStr,0,2)
        M=np.array(ret)
        seedsActTime={}
        startTime = 0
        infected=set()

        while M.shape[0] > 0:
            ind=np.argmin(M,0)[0]
            nodeID = M[ind][1]
            time = self.convertUnixTime(M[ind][0])
            friends=self.cares(nodeID)  # find the nodes can infect M[ind][1]
            fa=self.list2array(friends)
            
            seeds=[]
            for k in cascades.keys():
                interset = set(fa).intersection(set(cascades[k].treeSet()))
                # it has infected parents 
                if len(interset)>0:
                    seeds.append(k)
                    parent = list(interset)[0]  # ignore the multiple parents case, all these parents are in the same cascade, so it won't affect the cascade size
                    
                    # this means this node's friend is infected before
                    # add the node into cascade k
                    node = TREE(nodeID, cascades[k].node(parent).depth()+1, time)
                    cascades[k].node(parent).addChild(node)
                    
                    infected.add(nodeID)

            # it has no infected parents
            if len(seeds)==0:
                # this node is infected without any pre-infected friends
                if startTime == 0:
                    startTime = time
                node = TREE(nodeID, 1, time)
                cascades[nodeID]=node
                
                seedsActTime[nodeID]=self.hourDiff(time, startTime)
                
            M=np.delete(M,ind,0)
                        
        return (seedsActTime, infected, cascades)        
        
        
        
    def extractInfectionTimeDifferencePerLocation(self, story_id):
        cascades={}
        quote="\""
        qStr = "select vote_time, voter_id from connVotes where story_id=" + quote + str(story_id) + quote + " order by vote_time"
        ret=self.outputQuery(qStr,0,2)
        M=np.array(ret)
        locations={}
        #times={}

        while M.shape[0] > 0:
            ind=np.argmin(M,0)[0]
            nodeID = M[ind][1]
            time = self.convertUnixTime(M[ind][0])
            friends=self.cares(nodeID)  # find the nodes can infect M[ind][1]
            fa=self.list2array(friends)
            
            seeds=[]
            for k in cascades.keys():
                interset = set(fa).intersection(set(cascades[k].set())) 
                if len(interset)>0:
                    seeds.append(k)
                    interset=list(interset)
                    minhop=-1
                    parent=-1
                    for p in interset:
                        hop = cascades[k].node(p).depth()
                        if minhop==-1 or minhop > hop:
                            minhop=hop
                            parent = p
                        timediff = self.hourDiff(time, cascades[k].node(p).time())
                        if hop not in locations.keys():
                            locations[hop]={}
                            locations[hop][timediff] = 1
                        else:
                            if timediff not in locations[hop].keys():
                                locations[hop][timediff] = 1
                            else:
                                locations[hop][timediff] += 1
                          
                    #parent = list(interset)[0]  # ignore the multiple parents case, all these parents are in the same cascade, so it won't affect the cascade size
                    
                    # this means this node's friend is infected before
                    # add the node into cascade k
                    node = TREE(nodeID, cascades[k].node(parent).depth()+1, time)
                    cascades[k].node(parent).addChild(node)

            if len(seeds)==0:
                # this node is infected without any pre-infected friends
                node = TREE(nodeID, 1, time)
                cascades[nodeID]=node
                #cascades[M[ind][1]]=[[M[ind][1]],self.friendsNumU(M[ind][1])]
                
            M=np.delete(M,ind,0)
                        
        return (locations)  
            
        
        
        
    # the function extract all cascades
    def extractCascades(self, story_id):
        cascades={}
        quote="\""
        qStr = "select vote_time, voter_id from connVotes where story_id=" + quote + str(story_id) + quote + " order by vote_time"
        ret=self.outputQuery(qStr,0,2)
        M=np.array(ret)
        seedsHist={}
        infected=set()

        while M.shape[0] > 0:
            ind=np.argmin(M,0)[0]
            nodeID = M[ind][1]
            time = self.convertUnixTime(M[ind][0])
            friends=self.cares(nodeID)  # find the nodes can infect M[ind][1]
            fa=self.list2array(friends)
            
            seeds=[]
            for k in cascades.keys():
                interset = set(fa).intersection(set(cascades[k].treeSet())) 
                if len(interset)>0:
                    seeds.append(k)
                    parent = list(interset)[0]  # ignore the multiple parents case, all these parents are in the same cascade, so it won't affect the cascade size
                    
                    # this means this node's friend is infected before
                    # add the node into cascade k
                    node = TREE(nodeID, cascades[k].node(parent).depth()+1)
                    cascades[k].node(parent).addChild(node)
                    infected.add(nodeID)

            if len(seeds)==0:
                # this node is infected without any pre-infected friends
                node = TREE(nodeID, 1)
                cascades[nodeID]=node
                seedsHist[nodeID]=time
                #cascades[M[ind][1]]=[[M[ind][1]],self.friendsNumU(M[ind][1])]
                
            M=np.delete(M,ind,0)
                        
        return (cascades, infected, seedsHist)
    
        
    
        
    def extractCascadesProb(self, story_id):
        cascades={}
        quote="\""
        timeTab = self.storyHist(story_id)
        prob_dist={}
        seedsCount={}
        preTime = []
        startTime= []
        infNodes={}
        for curTime in timeTab:
            if len(preTime)==0:
                preTime=curTime
                startTime=curTime
            else:
                # find votes between preTime and curTime
                preTimeStr="2009-" + str(preTime[0]) + "-" + str(preTime[1]) + " " + str(preTime[2]) + ":00:00"
                curTimeStr="2009-" + str(curTime[0]) + "-" + str(curTime[1]) + " " + str(curTime[2]) + ":00:00" 
                qStr = "select vote_time, voter_id from connVotes where story_id=" + quote + str(story_id) + quote\
                    +  " and vote_time>=unix_timestamp(" + quote + preTimeStr + quote + ") and vote_time<unix_timestamp(" + quote + curTimeStr + quote + ")"\
                    +  " order by vote_time"
                ret=self.outputQuery(qStr,0,2)
                preTime = curTime
                M=np.array(ret)
        
                diff = self.hourDiff(startTime, curTime)
                prob_dist[diff]=[]
                seedsCount[diff]=0
                
                while M.shape[0] > 0:
                    ind=np.argmin(M,0)[0]
                    
                    nodeID=M[ind][1]
                    friends=self.cares(nodeID)  # find the nodes can infect nodeID
                    fa=self.list2array(friends)
                    
                    infNodes[nodeID]=[self.list2array(self.friends(nodeID)), []]  # find the nodes can be infected by nodeID
                    
                    seed=-1
                    for k in cascades.keys():
                        interset = set(fa).intersection(set(cascades[k])) 
                        if len(interset)>0:
                            seed=k
                            parent = list(interset)[0]  # ignore the multiple parents case
                            break
                    
                    if seed!=-1:
                        # this means this node's friend is infected before
                        #cascades[seed][0].append(M[ind][1])
                        cascades[seed].append(nodeID)
                        M=np.delete(M,ind,0)
                        infNodes[parent][1].append(nodeID)
                        
                    else:
                        # this node is infected without any pre-infected friends
                        cascades[nodeID]=[nodeID]
                        #cascades[M[ind][1]]=[[M[ind][1]],self.friendsNumU(M[ind][1])]
                        M=np.delete(M,ind,0)
                        seedsCount[diff] +=1 
                
                #print len(infNodes.keys()), seedsCount[diff] 
                for k in infNodes.keys():
                    if len(infNodes[k][1])>0:
                        #print k, len(infNodes[k][1]), len(infNodes[k][0])
                        prob_dist[diff].append(1.0*len(infNodes[k][1])/len(infNodes[k][0]))
                        #infNodes[k][0] = list( set(infNodes[k][0]) - set(infNodes[k][1]) )
                    else:
                        prob_dist[diff].append(0)
                    infNodes[k][1] = []
                         
        return (prob_dist, seedsCount)
        
        
        
    # the function try to extract how many nodes are infected along the time (per hour) and the depth this nodes belong to
    def extractCascadesSteps(self, story_id):
            cascades={}
            cascadesTime={}
            quote="\""
            timeTab = self.storyHist(story_id)
            steps_dist={}  # the key is the time diff, the content is also a directory, it stores the number of nodes infected which belong to depth-i   
            preTime = []
            startTime= []
            for curTime in timeTab:
                if len(preTime)==0:
                    preTime=curTime
                    startTime=curTime
                else:
                    # find votes between preTime and curTime
                    preTimeStr="2009-" + str(preTime[0]) + "-" + str(preTime[1]) + " " + str(preTime[2]) + ":00:00"
                    curTimeStr="2009-" + str(curTime[0]) + "-" + str(curTime[1]) + " " + str(curTime[2]) + ":00:00" 
                    qStr = "select vote_time, voter_id from connVotes where story_id=" + quote + str(story_id) + quote\
                        +  " and vote_time>=unix_timestamp(" + quote + preTimeStr + quote + ") and vote_time<unix_timestamp(" + quote + curTimeStr + quote + ")"\
                        +  " order by vote_time"
                    ret=self.outputQuery(qStr,0,2)
                    preTime = curTime
                    M=np.array(ret)
            
                    diff = self.hourDiff(startTime, curTime)
                    steps_dist[diff]={}
                    
                    
                    while M.shape[0] > 0:
                        ind=np.argmin(M,0)[0]
                        
                        nodeID=M[ind][1]
                        friends=self.cares(nodeID)  # find the nodes can infect nodeID
                        fa=self.list2array(friends)
                        
                        seed=-1
                        for k in cascades.keys():
                            interset = set(fa).intersection(set(cascades[k].keys())) 
                            if len(interset)>0:
                                seed=k
                                parent = list(interset)[0]  # ignore the multiple parents case
                                break
                        
                        if seed!=-1:
                            # this means this node's friend is infected before
                            #cascades[seed][0].append(M[ind][1])
                            depth = cascades[seed][parent] + 1
                            cascades[seed][nodeID]=depth
                            M=np.delete(M,ind,0)
                            if depth not in steps_dist[diff].keys():
                                steps_dist[diff][depth]=1
                            else:
                                steps_dist[diff][depth] += 1       
                                
                            if diff > cascadesTime[seed][2]:
                                cascadesTime[seed][2]=diff
                            cascadesTime[seed][0] +=1
                            
                        else:
                            # this node is infected without any pre-infected friends
                            
                            cascades[nodeID]={}
                            cascadesTime[nodeID]=[1, diff, diff]   # the zero element is the size, the first element is the start time, the second is for the end time
                            cascades[nodeID][nodeID]=1
                            #cascades[M[ind][1]]=[[M[ind][1]],self.friendsNumU(M[ind][1])]
                            M=np.delete(M,ind,0)
                            if 1 not in steps_dist[diff].keys():
                                steps_dist[diff][1]=1
                            else:
                                steps_dist[diff][1] += 1
                    
                             
            return (steps_dist, cascadesTime)        
        
        
    def extractCascadesStepsOrig(self, story_id):
        # extract the t
            cascades={}  #each cascade is indexed with the seed ID; each cascade is also a dictionary, save the node's depth
            quote="\""
            timeTab = self.storyHist(story_id)
            steps_dist={}  # the key is the time diff, the content is also a directory, it stores the number of nodes infected which belong to depth-i, and its orign
            # the origin is the time diff which labels when its origin is infected.
            cascades_steps_dist={}  # compared to steps_dist, this is based on each cascade  
            # story_id | Time | Cascade_id | level | orig Time | Count
            preTime = []
            startTime= []
            cascadesNum=0
            cascadesNumMap={}
            
            for curTime in timeTab:
                if len(preTime)==0:
                    preTime=curTime
                    startTime=curTime
                else:
                    # find votes between preTime and curTime
                    preTimeStr="2009-" + str(preTime[0]) + "-" + str(preTime[1]) + " " + str(preTime[2]) + ":00:00"
                    curTimeStr="2009-" + str(curTime[0]) + "-" + str(curTime[1]) + " " + str(curTime[2]) + ":00:00" 
                    qStr = "select vote_time, voter_id from connVotes where story_id=" + quote + str(story_id) + quote\
                        +  " and vote_time>=unix_timestamp(" + quote + preTimeStr + quote + ") and vote_time<unix_timestamp(" + quote + curTimeStr + quote + ")"\
                        +  " order by vote_time"
                    ret=self.outputQuery(qStr,0,2)
                    preTime = curTime
                    M=np.array(ret)
            
                    diff = self.hourDiff(startTime, curTime)
                    steps_dist[diff]={}
                    cascades_steps_dist[diff]={}
                    
                    while M.shape[0] > 0:
                        ind=np.argmin(M,0)[0]
                        
                        nodeID=M[ind][1]
                        friends=self.cares(nodeID)  # find the nodes can infect nodeID
                        fa=self.list2array(friends)
                        
                        seed=-1
                        for k in cascades.keys():
                            interset = set(fa).intersection(set(cascades[k].keys())) 
                            if len(interset)>0:
                                seed=k
                                parent = list(interset)[0]  # ignore the multiple parents case
                                break
                        
                        if seed!=-1:
                            # this means this node's friend is infected before
                            #cascades[seed][0].append(M[ind][1])
                            depth = cascades[seed][parent][0] + 1
                            cascades[seed][nodeID]=[depth, diff]
                            M=np.delete(M,ind,0)
                            orig = cascades[seed][parent][1]  #the time its parent being infected
                            if depth not in steps_dist[diff].keys():
                                steps_dist[diff][depth]={}
                                steps_dist[diff][depth][orig]=1
                            else:
                                if orig not in steps_dist[diff][depth].keys():
                                    steps_dist[diff][depth][orig] = 1
                                else:
                                    steps_dist[diff][depth][orig] += 1
                        
                                
                            if cascadesNumMap[seed] not in cascades_steps_dist[diff].keys():
                                cascades_steps_dist[diff][cascadesNumMap[seed]]={}
                                
                            if depth not in cascades_steps_dist[diff][cascadesNumMap[seed]].keys():
                                cascades_steps_dist[diff][cascadesNumMap[seed]][depth]={}
                                cascades_steps_dist[diff][cascadesNumMap[seed]][depth][orig]=1
                            else:
                                if orig not in cascades_steps_dist[diff][cascadesNumMap[seed]][depth].keys():
                                    cascades_steps_dist[diff][cascadesNumMap[seed]][depth][orig] = 1
                                else:
                                    cascades_steps_dist[diff][cascadesNumMap[seed]][depth][orig] += 1
                            
                        else:
                            # this node is infected without any pre-infected friends
                            
                            
                            #cascades[M[ind][1]]=[[M[ind][1]],self.friendsNumU(M[ind][1])]
                            M=np.delete(M,ind,0)
                            if 1 not in steps_dist[diff].keys():
                                steps_dist[diff][1]={}
                                steps_dist[diff][1][0]=1
                            else:
                                steps_dist[diff][1][0] += 1
                    
                            if nodeID not in cascades.keys():
                                cascades[nodeID]={}
                                cascades[nodeID][nodeID]=[1,diff]
                                cascadesNum += 1
                                cascadesNumMap[nodeID]=cascadesNum
                                
                            if cascadesNum not in cascades_steps_dist[diff].keys():
                                cascades_steps_dist[diff][cascadesNum]={}
                                cascades_steps_dist[diff][cascadesNum][1]={}
                                cascades_steps_dist[diff][cascadesNum][1][0]=1
                    
                             
            return (steps_dist, cascades_steps_dist)                
        


    def extractCascadesStepsOrigProb(self, story_id):
        # extract the t
            cascades={}  #each cascade is indexed with the seed ID; each cascade is also a dictionary, save the node's depth
            quote="\""
            timeTab = self.storyHist(story_id)
            steps_dist={}  # the key is the time diff, the content is also a directory, it stores the number of nodes infected which belong to depth-i, and its orign
            # the origin is the time diff which labels when its origin is infected.
            cascades_steps_dist={}  # compared to steps_dist, this is based on each cascade  
            # story_id | Time | Cascade_id | level | orig Time | Count
            preTime = []
            startTime= []
            cascadesNum=0
            cascadesNumMap={}
            
            for curTime in timeTab:
                if len(preTime)==0:
                    preTime=curTime
                    startTime=curTime
                else:
                    # find votes between preTime and curTime
                    preTimeStr="2009-" + str(preTime[0]) + "-" + str(preTime[1]) + " " + str(preTime[2]) + ":00:00"
                    curTimeStr="2009-" + str(curTime[0]) + "-" + str(curTime[1]) + " " + str(curTime[2]) + ":00:00" 
                    qStr = "select vote_time, voter_id from connVotes where story_id=" + quote + str(story_id) + quote\
                        +  " and vote_time>=unix_timestamp(" + quote + preTimeStr + quote + ") and vote_time<unix_timestamp(" + quote + curTimeStr + quote + ")"\
                        +  " order by vote_time"
                    ret=self.outputQuery(qStr,0,2)
                    preTime = curTime
                    M=np.array(ret)
            
                    diff = self.hourDiff(startTime, curTime)
                    steps_dist[diff]={}
                    cascades_steps_dist[diff]={}
                    
                    while M.shape[0] > 0:
                        ind=np.argmin(M,0)[0]
                        
                        nodeID=M[ind][1]
                        friends=self.cares(nodeID)  # find the nodes can infect nodeID
                        fa=self.list2array(friends)
                        
                        seed=-1
                        for k in cascades.keys():
                            interset = set(fa).intersection(set(cascades[k].keys())) 
                            if len(interset)>0:
                                seed=k
                                parent = list(interset)[0]  # ignore the multiple parents case
                                potential = self.list2array(self.friends(parent))
                                break
                        
                        if seed!=-1:
                            # this means this node's friend is infected before
                            #cascades[seed][0].append(M[ind][1])
                            depth = cascades[seed][parent][0] + 1
                            cascades[seed][nodeID]=[depth, diff]
                            M=np.delete(M,ind,0)
                            orig = cascades[seed][parent][1]  #the time its parent being infected                        
                                
                            if cascadesNumMap[seed] not in cascades_steps_dist[diff].keys():
                                cascades_steps_dist[diff][cascadesNumMap[seed]]={}
                                
                            if depth not in cascades_steps_dist[diff][cascadesNumMap[seed]].keys():
                                cascades_steps_dist[diff][cascadesNumMap[seed]][depth]={}
                                cascades_steps_dist[diff][cascadesNumMap[seed]][depth][orig]=[1, potential]
                            else:
                                if orig not in cascades_steps_dist[diff][cascadesNumMap[seed]][depth].keys():
                                    cascades_steps_dist[diff][cascadesNumMap[seed]][depth][orig] = [1, potential]
                                else:
                                    # combine the potential nodes could be infected of parents
                                    pot = cascades_steps_dist[diff][cascadesNumMap[seed]][depth][orig][1]
                                    cascades_steps_dist[diff][cascadesNumMap[seed]][depth][orig][1] = list(set(pot + potential))
                                    cascades_steps_dist[diff][cascadesNumMap[seed]][depth][orig][0] += 1
                        else:
                            # this node is infected without any pre-infected friends
                            
                            
                            #cascades[M[ind][1]]=[[M[ind][1]],self.friendsNumU(M[ind][1])]
                            M=np.delete(M,ind,0)
                    
                            if nodeID not in cascades.keys():
                                cascades[nodeID]={}
                                cascades[nodeID][nodeID]=[1,diff]
                                cascadesNum += 1
                                cascadesNumMap[nodeID]=cascadesNum
                                
                            if cascadesNum not in cascades_steps_dist[diff].keys():
                                cascades_steps_dist[diff][cascadesNum]={}
                                cascades_steps_dist[diff][cascadesNum][1]={}
                                cascades_steps_dist[diff][cascadesNum][1][0]=[1,[]]
                    
                             
            return (steps_dist, cascades_steps_dist)      



    
    def penNeighborVoting(self, story_id):
        quote="\""
        qStr = " select friends.user_id as userid, count(friends.friend_id) as fvotes from friends inner join connVotes as cv1 on friends.user_id=cv1.voter_id"\
               " inner join connVotes as cv2 on friends.friend_id=cv2.voter_id where cv1.story_id=" + quote + str(story_id) + quote +\
               " and cv2.story_id=" + quote + str(story_id) + quote + " and cv1.vote_time > cv2.vote_time group by friends.user_id "
        # voted returns a list of two elements vector. Each vector is [x,y]  x is the user_id (x voted), y is the number of neighbors which voted before x.        
        voted=self.outputQuery(qStr,0,2)
        
        qStr = " select ft.userid, ft.fvotes from (select count(friends.friend_id) as fvotes, friends.user_id as userid from friends inner join connVotes "\
               " on friends.friend_id=connVotes.voter_id where story_id=" + quote + str(story_id) + quote + " group by friends.user_id) as ft;"
        # voted returns a list of two elements vector. Each vector is [x,y]  x is the user_id (x may not voted), y is the total number of neighbors of x voted.        
        total=self.outputQuery(qStr,0,2)
        return (voted, total)
    
    
    def penNeighborVotingIT(self, story_id, option):
        quote="\""
        if option == 0:
            qStr = " select friends.user_id as userid, count(friends.friend_id) as fvotes, users.friends_num, users.cares_num from friends inner join users on friends.user_id = users.user_id inner join connVotes as cv1 on friends.user_id=cv1.voter_id"\
                   " inner join connVotes as cv2 on friends.friend_id=cv2.voter_id where cv1.story_id=" + quote + str(story_id) + quote +\
                   " and cv2.story_id=" + quote + str(story_id) + quote + " and cv1.vote_time > cv2.vote_time group by friends.user_id "
            # voted returns a list of two elements vector. Each vector is [x,y]  x is the user_id (x voted), y is the number of neighbors which voted before x.        
            voted=self.outputQuery(qStr,0,4)
            
            qStr = " select ft.userid, ft.fvotes from (select count(friends.friend_id) as fvotes, friends.user_id as userid from friends inner join connVotes "\
                   " on friends.friend_id=connVotes.voter_id where story_id=" + quote + str(story_id) + quote + " group by friends.user_id) as ft;"
            # voted returns a list of two elements vector. Each vector is [x,y]  x is the user_id (x may not voted), y is the total number of neighbors of x voted.        
            total=self.outputQuery(qStr,0,2)
            return (voted, total)
        elif option == 1:
            qStr = " select friends.user_id as userid, count(friends.friend_id) as fvotes from friends inner join connVotes as cv1 on friends.user_id=cv1.voter_id"\
                   " inner join connVotes as cv2 on friends.friend_id=cv2.voter_id where cv1.story_id=" + quote + str(story_id) + quote +\
                   " and cv2.story_id=" + quote + str(story_id) + quote + " and cv1.vote_time > cv2.vote_time group by friends.user_id "
            # voted returns a list of two elements vector. Each vector is [x,y]  x is the user_id (x voted), y is the number of neighbors which voted before x.        
            voted=self.outputQuery(qStr,0,2)
                        
            qStr = " select ft.userid, ft.fvotes from (select count(friends.friend_id) as fvotes, friends.user_id as userid from friends inner join connVotes "\
                   " on friends.friend_id=connVotes.voter_id where story_id=" + quote + str(story_id) + quote + " group by friends.user_id) as ft;"
            # voted returns a list of two elements vector. Each vector is [x,y]  x is the user_id (x may not voted), y is the total number of neighbors of x voted.        
            total=self.outputQuery(qStr,0,2)
            return (voted, total)            
                
                
    def componentsNum(self, node, story_id, option):
        quote = "\""
        if option=='voted':
            qStr = " select friends.friend_id from friends inner join connVotes as cv1 on friends.user_id=cv1.voter_id inner join connVotes as cv2 "\
                " on friends.friend_id=cv2.voter_id where friends.user_id=" + quote + str(node) + quote + " and cv1.story_id=" + quote + str(story_id) + quote\
                + " and cv2.story_id=" + quote + str(story_id) + quote + " and cv1.vote_time > cv2.vote_time "
        elif option=='total':
            qStr = " select friends.friend_id from friends inner join connVotes as cv on friends.friend_id=cv.voter_id and friends.user_id="\
                   + quote + str(node) + quote + " and cv.story_id=" + quote + str(story_id) + quote
        
        ret=self.outputQuery(qStr,0,1)
        nodelist = self.list2array(ret)        
        
        friendsDict={}
        for n in nodelist:
            friends = self.friends(n)
            friendsDict[n]=self.list2array(friends)
            
        components={}
        for n in nodelist:
            attach=[]
            for k in components.keys():
                c = components[k]
                friends = friendsDict[n]
                if len(set(friends).intersection(set(c)))>0:
                    # the node n can reach the component c
                    attach.append(k)
                else:
                    # test whether the component can reach node n
                    for l in c:
                        lf = friendsDict[l]
                        if len(set([n]).intersection(set(lf)))>0:
                            # the node n can reach the component c
                            attach.append(k)
                            break                        
            # merge components in attach together
            components[n]=[]
            for a in attach:
                components[n] = list(set(components[n] + components[a]))  # unique list
                del components[a]
            components[n].append(n)  # if n does not connect to any existed components, create itself as a component
    
        return len(components)
    
        
    #extract the histogram information of the story
    # start time, endtime, and count of votes at each hour
    def storyHist(self, story_id):
        quote="\""
        qStr = "select month(from_unixtime(vote_time)) as month,  day(from_unixtime(vote_time)) as day, hour(from_unixtime(vote_time)) as hour, count(*)"\
            + " from connVotes where story_id=" + quote + str(story_id) + quote + " group by month(from_unixtime(vote_time)),  day(from_unixtime(vote_time)),"\
            + " hour(from_unixtime(vote_time))"
        ret=self.outputQuery(qStr,0,4)        
        
        return ret
        
        
    def hourDiff(self, time1, time2):
        # time is in a list [month, day, hour]
        quote="\""
        time2str = "2009-" + str(time2[0]) + "-" + str(time2[1]) + " " + str(time2[2]) + ":00:00"
        time1str = "2009-" + str(time1[0]) + "-" + str(time1[1]) + " " + str(time1[2]) + ":00:00"
        qStr = "select hour(timediff(" + quote +  time2str + quote + "," + quote +  time1str + quote + "))"
        ret=self.outputQuery(qStr,0,1)        
        
        return ret[0][0]
            
    def firstVoteTime(self):
        qStr="select month(from_unixtime(min(vote_time))), day(from_unixtime(min(vote_time))),hour(from_unixtime(min(vote_time))) from connVotes"
        ret=self.outputQuery(qStr,0,3)
        return ret[0]
    
    def firstVoteTimeStory(self, story_id):
        quote="\""
        qStr="select month(from_unixtime(min(vote_time))), day(from_unixtime(min(vote_time))),hour(from_unixtime(min(vote_time))) from connVotes"\
             + " where story_id=" + quote + str(story_id) + quote
        ret=self.outputQuery(qStr,0,3)
        return ret[0]    
    
    def convertUnixTime(self, unixTime):
        quote="\""
        qStr="select month(from_unixtime(" + quote + str(unixTime) + quote + ")), day(from_unixtime(" + quote + str(unixTime) + quote +\
             ")),hour(from_unixtime(" + quote + str(unixTime) + quote + "))"
        ret=self.outputQuery(qStr,0,3)
        return ret[0]
    
        
    def initialNodes(self, story_id):
        
        quote="\""
        qStr = "select vote_time, voter_id from connVotes where story_id=" + quote + str(story_id) + quote
        ret=self.outputQuery(qStr,0,2)
        M=np.array(ret)
        cascade_size=M.shape[0]
        initial_set=[]
        infected_set=[]
        while M.shape[0] > 0:
            ind=np.argmin(M,0)[0]
            
            friends=self.cares(M[ind][1])  # find the nodes can infect the M[ind][1]
            fa=self.list2array(friends)
            if len(set(fa).intersection(set(infected_set)))>0:
                # this means this node's friend is infected before
                infected_set.append(M[ind][1])
                M=np.delete(M,ind,0)
            else:
                # this node is infected without any pre-infected friends
                infected_set.append(M[ind][1])
                initial_set.append(M[ind][1])
                M=np.delete(M,ind,0)
        
        
        return (len(initial_set), initial_set, cascade_size, infected_set)
        
        
    def friends(self, uid):
        # we are going to find outgoing links, the users can be infected by the uid
        quote="\""
        qStr="select user_id from friends where friend_id=" + quote + str(uid) + quote
        ret=self.outputQuery(qStr,0,1)
        return ret
    
    
    def validFriends(self, uid):
        # we are going to find outgoing links, the users can be infected by the uid
        quote="\""
        qStr="select user_id from friends where friend_id=" + quote + str(uid) + quote + " and prob_same_act >= 0"
        ret=self.outputQuery(qStr,0,1)
        return ret    
    
    
    def cares(self, uid):
        # we are going to find incoming links, the users can infect the uid
        
        quote="\""
        qStr="select friend_id from friends where user_id=" + quote + str(uid) + quote
        ret=self.outputQuery(qStr,0,1)
        return ret       
    
    def giantCandidates(self, degree):
        quote="\""
        qStr="select user_id from users where friends_num>=" + quote + str(degree) + quote
        ret=self.outputQuery(qStr,0,1)
        return ret   
    
    
    def giantCandidatesActProb(self):
        qStr="create view temp as select friends.friend_id, exp(sum(ln(1-prob_same_act))) as prob from friends group by friends.friend_id"
        self.outputQuery(qStr,0,1)
        
        qStr="select friend_id from temp where prob<0.01"
        ret=self.outputQuery(qStr,0,1)
        
        qStr="drop view temp"
        self.outputQuery(qStr,0,1)
        return ret
    
    
    def giantCandidatesNetInfProb(self):
        qStr="create view temp as select friends.friend_id, exp(sum(ln(1-prob_netinf))) as prob from friends group by friends.friend_id"
        self.outputQuery(qStr,0,1)
        
        qStr="select friend_id from temp where prob<0.01"
        ret=self.outputQuery(qStr,0,1)
        
        qStr="drop view temp"
        self.outputQuery(qStr,0,1)
        return ret
    
    
    def userId(self, c):
        quote="\""
        qStr="select user_id from users where uid=" + quote + str(c) + quote
        ret=self.outputQuery(qStr,0,1)[0][0]
        return ret
    
    def friendsNum(self, c):
        quote="\""
        qStr="select friends_num from users where uid=" + quote + str(c) + quote
        ret=self.outputQuery(qStr,0,1)[0][0]
        return ret
    
    def friendsNumU(self, c):
        quote="\""
        qStr="select friends_num from users where user_id=" + quote + str(c) + quote
        ret=self.outputQuery(qStr,0,1)[0][0]
        return ret    
    
    def actProbHist(self, s, t):
        quote="\""
        qStr="select count(*) from friends where prob_same_act>=" + quote + str(s) + quote + " and prob_same_act<" + quote + str(t) + quote
        ret=self.outputQuery(qStr,0,1)[0][0]
        return ret    
    
    def caresNum(self, c):
        quote="\""
        qStr="select cares_num from users where uid=" + quote + str(c) + quote
        ret=self.outputQuery(qStr,0,1)[0][0]
        return ret
    
    def caresNumU(self, c):
        quote="\""
        qStr="select cares_num from users where user_id=" + quote + str(c) + quote
        ret=self.outputQuery(qStr,0,1)[0][0]
        return ret
    
    def updateFriendsNum(self):  # how many people the user can infect
        N=self.usersNum()
        quote="\""
        for i in range(N):
            qStr="set @a:=( select count(*) from friends inner join users on friends.friend_id=users.user_id and users.uid=" + quote + str(i+1) + quote + ")"
            self.v_cur.execute(qStr)
            qStr="update users set friends_num=@a where uid=" + quote + str(i+1) + quote
            self.v_cur.execute(qStr)
            
        self.commit()
        
        
    def reachabilityDeep(self, uid, accessed):
        reachable=set()
        accessed.add(uid)
        quote="\""
        qStr="select user_id from friends where friend_id=" + quote + str(uid) + quote
        ret=self.outputQuery(qStr,0,1)
        for child in ret:
            if child[0] not in accessed:
                child_reachable = self.reachabilityDeep(child[0], accessed)
                reachable = reachable | child_reachable
                accessed.add(child[0])
        return reachable
        
        
    def reachability(self, uid): 
        accessed=set()
        proceeding=set()
        proceeding.add(uid)
        quote="\""
        
        while len(proceeding):
            obj = list(proceeding)[0]
            qStr="select user_id from friends where friend_id=" + quote + str(obj) + quote
            ret=self.outputQuery(qStr,0,1)
            for child in ret:
                if child[0] not in list(accessed):
                    proceeding.add(child[0])
                    
            accessed.add(obj)
            proceeding.remove(obj)
        return len(accessed)-1
        
        
    def updateReachability(self):
        N=self.usersNum()
        quote="\""
        users = self.allUsers()
        i=1
        for user in users:
            
            #reachable = self.reachability(user[0])
            reachable = self.reachabilityDeep(user[0], set())
            print i, "/", N, ": ", len(reachable)
            qStr="update users set reachability=" + quote + str(len(reachable)) + quote + " where user_id=" + quote + str(user[0]) + quote
            self.v_cur.execute(qStr)
            i +=1
            
        self.commit()
        
        
    def updateCaresNum(self):  # how many people can infect the user
        N=self.usersNum()
        quote="\""
        for i in range(N):
            qStr="set @a:=( select count(*) from friends inner join users on friends.user_id=users.user_id and users.uid=" + quote + str(i+1) + quote + ")"
            self.v_cur.execute(qStr)
            qStr="update users set cares_num=@a where uid=" + quote + str(i+1) + quote
            self.v_cur.execute(qStr)
            
        self.commit()    
    
    
    def updateProbabilitySameAct(self):
        # the table friends stores the edge where frined_id (name as userA), if userA does something, it will affect current user do something. so the direction of the edge is from 
        # frined_id to user_id
        quote="\""
        M=self.edgesNum()
        for i in range(M):
            print i,M
            qStr=" create view votesA as select * from votes where votes.voter_id=(select friend_id from friends where rid=" + quote + str(i+1) + quote + ")"
            self.v_cur.execute(qStr)
        
            qStr=" create view votesB as select * from votes where votes.voter_id=(select user_id from friends where rid=" + quote + str(i+1) + quote + ")"
            self.v_cur.execute(qStr)
        
            qStr=" select count(*) from votesA inner join votesB on votesA.story_id=votesB.story_id and votesA.vote_time<votesB.vote_time"
            A2B=self.outputQuery(qStr,0,1)[0][0]
        
            qStr= " select count(*) from votesA"
            A=self.outputQuery(qStr,0,1)[0][0]
        
            
            rate=0
            if A!=0:
                rate=1.0*A2B/A
                
            qStr= " update friends set prob_same_act=" + quote + str(rate) + quote + " where rid=" + quote + str(i+1) + quote
            self.v_cur.execute(qStr)
            
            qStr= " drop view votesA"
            self.v_cur.execute(qStr)
            
            qStr= " drop view votesB"
            self.v_cur.execute(qStr)
            
        self.commit()
        
        
    def updateProbanilityNetInf(self):
        # 
        quote="\""
        M=self.edgesNum()
        for i in range(M):
            qStr=" create view votesA as select * from connVotes where connVotes.voter_id=(select friend_id from friends where rid=" + quote + str(i+1) + quote + ")"
            self.v_cur.execute(qStr)
        
            qStr=" create view votesB as select * from connVotes where connVotes.voter_id=(select user_id from friends where rid=" + quote + str(i+1) + quote + ")"
            self.v_cur.execute(qStr)
            
            qStr=" select 1-exp(sum(log(1-exp(-abs( hour(timediff(from_unixtime(votesA.vote_time), from_unixtime(votesB.vote_time)))" \
                + " + minute(timediff(from_unixtime(votesA.vote_time), from_unixtime(votesB.vote_time)))/60 "\
                + " ))))) from votesA inner"\
                + " join votesB on votesA.story_id=votesB.story_id and votesA.vote_time<votesB.vote_time"
            rate=self.outputQuery(qStr,0,1)[0][0]
            
            qStr= " update friends set prob_netinf=" + quote + str(rate) + quote + " where rid=" + quote + str(i+1) + quote
            self.v_cur.execute(qStr)
            
            qStr= " drop view votesA"
            self.v_cur.execute(qStr)
            
            qStr= " drop view votesB"
            self.v_cur.execute(qStr)
            
            print i,M,rate
            
        self.commit()     
            
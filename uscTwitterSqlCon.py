import MySQLdb as mdb
import sys
import time
import math      
import random
import numpy as np
from tree import TREE

# This database contains 3 tables:
# mysql> describe tweets;
# +-----------------------+------------+------+-----+---------+----------------+
# | Field                 | Type       | Null | Key | Default | Extra          |
# +-----------------------+------------+------+-----+---------+----------------+
# | tid                   | int(11)    | NO   | PRI | NULL    | auto_increment |
# | link                  | text       | YES  |     | NULL    |                |
# | id                    | bigint(20) | YES  | MUL | NULL    |                |
# | create_at             | datetime   | YES  | MUL | NULL    |                |
# | create_at_long        | bigint(20) | YES  |     | NULL    |                |
# | inreplyto_screen_name | text       | YES  |     | NULL    |                |
# | inreplyto_user_id     | int(11)    | YES  |     | NULL    |                |
# | source                | text       | YES  |     | NULL    |                |
# | bad_user_id           | int(11)    | YES  |     | NULL    |                |
# | user_screen_name      | text       | YES  |     | NULL    |                |
# | order_of_users        | int(11)    | YES  |     | NULL    |                |
# | user_id               | int(11)    | YES  | MUL | NULL    |                |
# +-----------------------+------------+------+-----+---------+----------------+
# link is the URL contained in the tweets 
#
# 
# mysql> describe active_follower_real;
# +-------------+---------+------+-----+---------+-------+
# | Field       | Type    | Null | Key | Default | Extra |
# +-------------+---------+------+-----+---------+-------+
# | user_id     | int(11) | NO   | PRI | NULL    |       |
# | follower_id | int(11) | NO   | PRI | NULL    |       |
# +-------------+---------+------+-----+---------+-------+
# 
# 
# mysql> describe users;
# +------------------+---------+------+-----+---------+----------------+
# | Field            | Type    | Null | Key | Default | Extra          |
# +------------------+---------+------+-----+---------+----------------+
# | uid              | int(11) | NO   | PRI | NULL    | auto_increment |
# | user_id          | int(11) | YES  | MUL | NULL    |                |
# | user_screen_name | text    | YES  |     | NULL    |                |
# | indegree         | int(11) | YES  |     | NULL    |                |
# | outdegree        | int(11) | YES  |     | NULL    |                |
# | bad_user_id      | int(11) | YES  |     | NULL    |                |
# +------------------+---------+------+-----+---------+----------------+



class USCTWITTERSQLCON:
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
        qStr="select count(*) from active_follower_real"
        ret=self.outputQuery(qStr,0,1)
        return ret[0][0]
        
    def linksNum(self):
        qStr="select count(distinct(link)) from tweets"
        ret=self.outputQuery(qStr,0,1)
        return ret[0][0]
    
    def linkURLs(self):
        qStr="select distinct(link) from tweets"
        ret=self.outputQuery(qStr,0,1)
        return ret
    
    def cares(self, uid):
        # we are going to find incoming links, the users can infect the uid    
        quote="\""
        qStr="select user_id from active_follower_real where follower_id=" + quote + str(uid) + quote
        ret=self.outputQuery(qStr,0,1)
        return ret     
    
    def convertUnixTime(self, unixTime):
        quote="\""
        qStr="select month(from_unixtime(" + quote + str(unixTime) + quote + ")), day(from_unixtime(" + quote + str(unixTime) + quote +\
             ")),hour(from_unixtime(" + quote + str(unixTime) + quote + "))"
        ret=self.outputQuery(qStr,0,3)
        return ret[0]    
    
    def list2array(self, l):
        a=[]
        for i in range(len(l)):
            a.append(l[i][0])
        return a    
    
    
    # the function extract seeds' activated time and which node is infected in this story
    # the time is relative compared to the start time of this story
    def extractInfection(self, link_url):
        cascades={}
        quote="\""
        qStr = "select create_at_long, user_id from tweets where link=" + quote + str(link_url) + quote + " order by create_at_long"
        ret=self.outputQuery(qStr,0,2)
        M=np.array(ret)
        seedsActTime={}
        startTime = 0
        infected=set()

        while M.shape[0] > 0:
            ind=np.argmin(M,0)[0]
            nodeID = M[ind][1]
            time = self.convertUnixTime(M[ind][0]/1000)
            friends=self.cares(nodeID)  # find the nodes can infect M[ind][1]
            fa=self.list2array(friends)
            
            seeds=[]
            for k in cascades.keys():
                interset = set(fa).intersection(set(cascades[k].set()))
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
    
    
    
    def hourDiff(self, time1, time2):
        # time is in a list [month, day, hour]
        quote="\""
        time2str = "2009-" + str(time2[0]) + "-" + str(time2[1]) + " " + str(time2[2]) + ":00:00"
        time1str = "2009-" + str(time1[0]) + "-" + str(time1[1]) + " " + str(time1[2]) + ":00:00"
        qStr = "select hour(timediff(" + quote +  time2str + quote + "," + quote +  time1str + quote + "))"
        ret=self.outputQuery(qStr,0,1)        
        
        return ret[0][0]             
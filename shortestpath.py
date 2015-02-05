
from operator import itemgetter
from igraph import *
import numpy as np
#from pqdict import PQDict
from priodict import priorityDictionary
import time
from sys import getsizeof

INFINITY=1<<32
T=math.fabs(math.log(0.001))
MAXPATHS=10

## Computes K paths from a source to a sink in the supplied graph.
#
# @param graph A digraph of class Graph.
# @param start The source node of the graph.
# @param sink The sink node of the graph.
# @param K The amount of paths being computed.
#
# @retval [] Array of paths, where [0] is the shortest, [1] is the next 
# shortest, and so on.
#
def ksp_yen(graph, node_start, node_end, max_k):
    
    orig_WM=np.asmatrix(graph.get_adjacency(attribute="weight").data)
    WM=np.matrix(orig_WM)
    distances, previous = dijkstra(graph, WM, node_start, None)
    
    A = [{'cost': distances[node_end], 
          'path': path(previous, node_start, node_end)}]
    B = []
    
    if not A[0]['path']: return A
    
    for k in range(1, max_k):
        for i in range(0, len(A[-1]['path']) - 1):
            
            gm=graph.copy()
            WM=np.matrix(orig_WM)
            node_spur = A[-1]['path'][i]
            path_root = A[-1]['path'][:i+1]
            
            edges_removed = []
            for path_k in A:
                curr_path = path_k['path']
                if len(curr_path) > i and path_root == curr_path[:i+1]:
                    if gm.are_connected(curr_path[i], curr_path[i+1]):
                        gm.delete_edges((curr_path[i], curr_path[i+1]))
                    #edges_removed.append([curr_path[i], curr_path[i+1], cost])
            
            for node in path_root:
                if node !=node_spur:
                    # do not remove nodes, but set the link weight of all edges to this node to infinity
                    WM[node,:]=INFINITY
                    WM[:,node]=INFINITY 
            
            if (node_end==None):
                pass    
            
            path_spur = dijkstra(gm, WM, node_spur, node_end)
            
            if 'path' not in path_spur:
                pass
            if path_spur['path']:
                path_total = path_root[:-1] + path_spur['path']
                dist_total = distances[node_spur] + path_spur['cost']
                potential_k = {'cost': dist_total, 'path': path_total}
            
                if not (potential_k in B) and dist_total < T:
                    B.append(potential_k)
            
            #for edge in edges_removed:
            #    graph.add_edge(edge[0], edge[1], edge[2])
        
        if len(B):
            B = sorted(B, key=itemgetter('cost'))
            A.append(B[0])
            B.pop(0)
        else:
            break
    
    return A

## Computes the shortest path from a source to a sink in the supplied graph.
#
# @param graph A digraph of class Graph.
# @param node_start The source node of the graph.
# @param node_end The sink node of the graph.
#
# @retval {} Dictionary of path and cost or if the node_end is not specified,
# the distances and previous lists are returned.
# option decides whether use adjacency matrix or actual weight
def dijkstra(graph, node_start, node_end, option):
    distances = {}      
    previous = {}       
    #Q = PQDict.minpq()
    Q = priorityDictionary()
    N=graph.vcount()
    
    
    for v in xrange(N):
        distances[v] = INFINITY
        previous[v] = None
        Q[v] = INFINITY
    
    
    distances[node_start] = 0
    Q[node_start] = 0
    
    while len(Q):
        #v = Q.top()
        v=Q.smallest()
        if v == node_end: break
        
        outv = graph.neighbors(v, "out")
        for u in outv:
            eid = graph.get_eid(v,u,error=False)
            if eid==-1:
                d=INFINITY
            else:
                if option==1:
                    d=1
                else:
                    d=graph.es[eid]["weight"]
            cost_vu = distances[v] + d
            
            if cost_vu < distances[u]:
                distances[u] = cost_vu
                Q[u] = cost_vu
                previous[u] = v
        del Q[v]
        

    if node_end!=None:
        return {'cost': distances[node_end], 
                'path': path(previous, node_start, node_end)}
    else:
        return (distances, previous)




def floydWarshall(g, N):
    dist=2*N*np.ones(shape=(N,N))
    AM = g.get_adjacency()
    for i in xrange(N):
        for j in xrange(N):
            if i!=j:
                if AM[i,j]==1:
                    dist[i,j]=g.es[g.get_eid(i,j)]
            else:
                dist[i,j]=0
                
    for k in xrange(N):
        for i in xrange(N):
            for j in xrange(N):
                if dist[i,j] > dist[i,k] + dist[k,j]:
                    dist[i,j] = dist[i,k] + dist[k,j]
                    
    return dist


## Finds a paths from a source to a sink using a supplied previous node list.
#
# @param previous A list of node predecessors.
# @param node_start The source node of the graph.
# @param node_end The sink node of the graph.
#
# @retval [] Array of nodes if a path is found, an empty list if no path is 
# found from the source to sink.
#
def path(previous, node_start, node_end):
    route = []

    node_curr = node_end    
    while True:
        route.append(node_curr)
        if previous[node_curr] == node_start:
            route.append(node_start)
            break
        elif previous[node_curr] == None:
            return []
        
        node_curr = previous[node_curr]
    
    route.reverse()
    return route


def transformLinkWeight(g):
    E=g.ecount()
    for i in xrange(E):
        g.es[i]["weight"]=math.fabs(math.log(g.es[i]["weight"]))  # the path with smallest link weight is the path which has max influence 
        

def reachabilityMulti(seed, g, rMulti):
    N=g.vcount()
    
    
    for i in xrange(N):
        print seed, i
        if i!=seed:
            paths = ksp_yen(g,seed,i,MAXPATHS)
            print paths
            p=1
            for path in paths:
                print path["cost"]
                p=p*(1-math.exp(-path["cost"]))
            rMulti[seed,i]=1-p
            print rMulti[seed,i]
    


def main():    
    g=Graph(directed=True)
    for i in xrange(10):
        g.add_vertices(str(i))
    g.add_edges([(0,5), (0,8), (1,5), (1,8), (1,9), (3,6), (4,0), (4,6), (4,8), (5,7), (5,8), (5,9), (5,3), \
                 (6,0), (6,9), (7,4), (7,8), (8,5), (8,2), (9,5)])

    N=g.vcount()
    E=g.ecount()

#    g = Graph.Static_Power_Law(N,E,2.45,2.45)
    for i in xrange(N):
        g.vs[i]["name"]=str(i)
    for i in xrange(E):
        g.es[i]["weight"]=0.5
    
    N=g.vcount()
    
    outNeighbors={}
    for i in xrange(N):
        outNeighbors[i] = g.neighbors(i, "out")    
    
    rMulti=np.zeros([N, N])
    for seed in xrange(1,2):    
        reachabilityMulti(seed, g, rMulti)
        print seed, (time.strftime("%H:%M:%S"))
    print "multi done", (time.strftime("%H:%M:%S"))

    print rMulti
if __name__=="__main__":
    main()    
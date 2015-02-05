# temporalDecay
temporal effect on information propagation

it’s based on the assumption that seeds activated at different time have different influence.

files:

priodict.py:	priority dictionary

shortestpath.py:  dijkstra algorithm

tree.py:	tree node

diggSqlCon.py:  the interface class to operate Digg mysql database
distances.py:  	calculate shortest distance from selected seeds to all other nodes
prepare.py:	
g.py: 		calculate function g with digg dataset
gradDescent.py: gradient descent with function g.
 

g_twitter.py:  calculate function g with twitter dataset
uscTwitterSqlCon.py:	the interface class to operate Twitter mysql database

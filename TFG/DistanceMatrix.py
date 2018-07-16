import math
import numpy as np
from operator import add
import rlcompleter, readline
readline.parse_and_bind('tab:complete')

 
def distanceMatrix(rdd,distance):
 rdd.cache()
 data=rdd.cartesian(rdd).filter(lambda (x,y):x[0]<y[0])
 data=data.map(lambda (x,y):((x[0],y[0]),[x[1:],y[1:]]))
 measure = {
	"Manhattan":data.map(lambda (x,y):((x[1],y[1][0][1]),[manhattan_dist(y[0][0][0],y[1][0][0]),y[0][0][1]])).groupByKey(),
	"Euclidean":data.map(lambda (x,y):((x[1],y[1][0][1]),[euclidean_dist(y[0][0][0],y[1][0][0]),y[0][0][1]])).groupByKey(),
	"Canberra":data.map(lambda (x,y):((x[1],y[1][0][1]),[canberra_dist(y[0][0][0],y[1][0][0]),y[0][0][1]])).groupByKey()
		 }
 return measure.get(distance,"Wrong distance")

 
 
#Devuelve la distancia euclidea entre 2 vectores
def euclidean_dist(x,y):
    dist = 0
    for i in range(0, len(x) if len(x) > len(y) else len(y)):
        dist += (x[i] - y[i])**2
    return math.sqrt(dist)
	
	
	
def manhattan_dist(x,y):
 dist = 0
 for i in range(0, len(x) if len(x) > len(y) else len(y)):
  dist += abs(x[i] - y[i])
 return dist

 
 
def canberra_dist(x,y):
 dist = 0
 for i in range(0, len(x) if len(x) > len(y) else len(y)):
  if(x[i]!=0 or y[i]!=0):
   dist += (abs(x[i] - y[i]))/(abs(x[i])+abs(y[i]))
 return dist

 
 
 #Elimina la cabecera de un RDD de entrada si lo tiene. 
def deleteHeader(idx, iter):
    output=[]
    for sublist in iter:
        output.append(sublist)
    if idx>0:
        return(output)
    else:
        return(output[1:])

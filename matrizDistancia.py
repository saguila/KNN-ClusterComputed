import math
import numpy as np
from operator import add
import rlcompleter, readline
readline.parse_and_bind('tab:complete')


#Genera la matriz de distancias
def distanceMatrix(rdd,distance):
 data=rdd.cartesian(rdd).filter(lambda (x,y):x[0]<y[0])
 data=data.map(lambda (x,y):((x[0],y[0]),[x[1:],y[1][0]]))
 measure = {
	"Manhattan":data.map(lambda (x,y):(x,[manhattan_dist(y[0],y[1]),y[0],y[1]])),
	"Euclidean":data.map(lambda (x,y):(x,[euclidea_dist(y[0][0][0],y[1]),y[0][0][1]])),
	"Canberra":data.map(lambda (x,y):(x,canberra_dist(y[0],y[1])))
		 }
 return measure.get(distance,"Wrong distance")

#Devuelve la distancia euclidea entre 2 vectores
def euclidea_dist(x,y):
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

		
#Devuelve las distancias de n de la matriz de distancias		
def getData(matrix,n):
 return matrix.filter(lambda (x,y):x[1]==n)

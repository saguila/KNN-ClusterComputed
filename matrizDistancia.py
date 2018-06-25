import math
import numpy as np
import rlcompleter, readline
readline.parse_and_bind('tab:complete')


#Genera la matriz de distancias
def distanceMatrix(rdd):
 data=rdd.cartesian(rdd).filter(lambda (x,y):x[0]<y[0])
 data=data.map(lambda (x,y):((x[0],y[0]),(x[1],y[1])))
 data=data.map(lambda (x,y):(x,euclidea_dist(y[0],y[1])))
 return data

 
#Devuelve la distancia euclidea entre 2 vectores
def euclidea_dist(x,y):
    dist = 0
    for i in range(0, len(x) if len(x) > len(y) else len(y)):
        dist += (x[i] - y[i])**2
    return math.sqrt(dist)
	
#Genera un RDD en funcion de la D
def KNN_Elements(rddInput, d):
	 rddList = []
	 rddInput = rddInput.zipWithIndex().map(lambda (x,y) : (y,defineOrder(x,0)))
	 rddList.append(rddInput)
	 for index in range(1,d):
	    	rddList.append(groupMapping(rddInput,index,d))
	 return spark.sparkContext.union(rddList).groupByKey().filter(lambda (x,y) : len(y) == d).mapValues(list).map(lambda(x,y):(x,sorted(y))).map(lambda(x,y):(x,orderList(y)))
	 
#Funcion de mapper: union de RDDs
def groupMapping(rdd,index,d):
    return rdd.map(lambda (x,y) : (x - index,[y[0]+index,y[1]]))
	

#Funcion de mapper:elimina las IDs de orden
def eraseOrder(x):
  k=list()
  for i in range(0,len(x)):
   k.append(x[i][1])
  return k
  
 
 #Funcion de mapper:Genera una ID de orden
def defineOrder(x,y):
 k=list()
 k.append(y)
 k.append(x)
 return k

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
 row=matrix.filter(lambda (x,y):x[0]==n)
 col=matrix.filter(lambda (x,y):x[1]==n)
 return row.union(col)
 
 
Xpredict = spark.sparkContext.textFile("hdfs:///loudacre/kb/Weather.csv").mapPartitionsWithIndex(deleteHeader).map(lambda x:np.array(x.split(","))).map(lambda x: np.float(x[1]))
d=3
rddTratado=dRddv2(Xpredict,d)
matrix=distanceMatrix(rddTratado)
matrix.take(10)
matrix.sortBy(lambda (x,y):y).collect()

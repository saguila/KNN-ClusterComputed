import numpy as np
import math

import rlcompleter, readline
readline.parse_and_bind('tab:complete')


def paralelizarPar(lon):
    x = []
    for i in range (lon):
        for j in range (i,lon):
            if(i != j):
                x.append((i,j))
    return x

def euclidea_dist(x,y):
    dist = 0
    for i in range(0, len(x) if len(x) > len(y) else len(y)):
        dist += (x[i] - y[i])**2
    return math.sqrt(dist)

def deleteHeader(idx, iter):
    output=[]
    for sublist in iter:
        output.append(sublist)
    if idx>0:
        return(output)
    else:
        return(output[1:])

def createIndexes(num):
        output=[]
        for i in range(num):
                for j in range(i):
                        output.append(i)
        return output

bitcoin = spark.sparkContext.textFile('hdfs:///loudacre/kb/bigBT.csv')
bitcoinWithoutHeader = bitcoin.mapPartitionsWithIndex(deleteHeader)
bitcoinWithoutHeader = bitcoinWithoutHeader.map(lambda line : np.array(line.split(',')).astype(np.float))
bitcoinIndexed = bitcoinWithoutHeader.zipWithIndex().map(lambda (x,y) : (y,x))
#indexes = spark.sparkContext.parallelize(range(t,0))
sizeData = bitcoinIndexed.count()
pares = spark.sparkContext.parallelize(paralelizarPar(sizeData)).map(lambda (x,y): (x,y))
fJoin = pares.join(bitcoinIndexed).map(lambda (x,y) : (y[0],(x,y[1])))
sJoin = fJoin.join(bitcoinIndexed).map(lambda(x,y):((x,y[0][0]),euclidea_dist(y[1],y[0][1])))
#sJoin tiene la matriz de distancias completa
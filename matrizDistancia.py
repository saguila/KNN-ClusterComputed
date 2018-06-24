import math
import numpy as np
import rlcompleter, readline
readline.parse_and_bind('tab:complete')



def distanceMatrix(rdd):
 data=rdd.cartesian(rdd).filter(lambda (x,y):x[0]<y[0])
 data=data.map(lambda (x,y):((x[0],y[0]),(x[1],y[1])))
 data=data.map(lambda (x,y):(x,euclidea_dist(y[0],y[1])))
 return data

 
 
def euclidea_dist(x,y):
    dist = 0
    for i in range(0, len(x) if len(x) > len(y) else len(y)):
        dist += (x[i] - y[i])**2
    return math.sqrt(dist)
	
	
def dRddv2(rddInput, d):
	 rddList = []
	 rddInput = rddInput.zipWithIndex().map(lambda (x,y) : (y,x))
	 rddList.append(rddInput)
	 for index in range(1,d):
	    	rddList.append(groupMapping(rddInput,index,d))
	 return spark.sparkContext.union(rddList).groupByKey().filter(lambda (x,y) : len(y) == d).mapValues(list).sortByKey(False,1)
	 
	 

def groupMapping(rdd,index,d):
    return rdd.map(lambda (x,y) : (x - index,y))
	
 
def mapDistance(x,cp):
  return cp.map(lambda (j,k):(j,euclidea_dist(x,k)))

  
def deleteHeader(idx, iter):
    output=[]
    for sublist in iter:
        output.append(sublist)
    if idx>0:
        return(output)
    else:
        return(output[1:])

		
Xpredict = spark.sparkContext.textFile("hdfs:///loudacre/kb/Weather.csv").mapPartitionsWithIndex(deleteHeader).map(lambda x:np.array(x.split(","))).map(lambda x: np.float(x[1]))
d=3
rddTratado=dRddv2(Xpredict,d)
matrix=distanceMatrix(rddTratado)
matrix.take(10)


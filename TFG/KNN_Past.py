from operations.Distance import Mdistance
from dRdd import dRdd
from pyspark.sql import SparkSession
from metrics.Weigths import mean
from utils.RDDUtils import reduction
'''
Predicts on a known past for a time serie using KNN-Algorithm, the predict values will be the instances from init+1 to n-1 
where n is the number of time stamps on the serie.
This values will be used to generate training and error validation predicitons.
@Params a time serie (rdd), the delay value d (d), the number of neighbours (k), init phase of the time serie (init), 
the distace type(distance), the mean weight type(weight).
@return a RDD with the predicitons made.
'''

def KNN_Past(rdd,d,k,n,distance="Euclidean",init=None,weight="Same"):
 spark = SparkSession.builder.getOrCreate()
 if(init == None):
	init=(n*2)/3
 data=dRdd(rdd,d,n)
 knn_train=(spark.sparkContext.broadcast(data.filter(lambda (x,y):x>init-d-1 and x!=n-d).collect()))
 matrix=data.mapPartitions(lambda it : Mdistance(it,knn_train,distance,k))
 return matrix.reduceByKey(lambda x,y:reduction(x,y,k)).sortByKey().map(lambda (x,y):mean(y,weight))
 

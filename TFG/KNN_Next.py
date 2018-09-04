from operations.Distance import Mdistance
from dRdd import dRdd
from pyspark.sql import SparkSession
from utils.RDDUtils import reduction
#from utils.PersistanceSingleton import PersistanceSingleton
from metrics.Weigths import mean
'''
Predicts on a unknown  value from a full time serie using KNN-Algorithm, the predict values will be the instance n+1
where n is the number of time stamps on the serie.
@Params a time serie (rdd), the delay value d (d), the number of neighbours (k)
the distace type(distance), the mean weight type(weight).
@return a prediction of a future instance.
'''

def KNN_Next(rdd,d,k,n,distance="Euclidean",weight="Same"): 
 spark = SparkSession.builder.getOrCreate()
 data=dRdd(rdd,d,n)
 knn_last=(spark.sparkContext.broadcast(data.filter(lambda (x,y):x==n-d).collect()))
 matrix=data.mapPartitions(lambda it : Mdistance(it,knn_last,distance,k))
 return matrix.reduceByKey(lambda x,y:reduction(x,y,k)).map(lambda (x,y):mean(y,weight)).collect()



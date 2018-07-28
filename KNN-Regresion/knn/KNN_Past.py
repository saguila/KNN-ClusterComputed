from operations.Distance import Mdistance
from dRdd import dRdd
from pyspark.sql import SparkSession
from metrics.Weigths import mean
from utils.RDDUtils import reduction


def KNN_Past(rdd,d,k,n,distance="Euclidean",init=None,weight="Same"):
 spark = SparkSession.builder.getOrCreate()
 if(init == None):
	init=(n*2)/3
 data=dRdd(rdd,d,n).cache()
 knn_train=(spark.sparkContext.broadcast(data.filter(lambda (x,y):x>init-d-1 and x!=n-d).collect()))
 matrix=data.mapPartitions(lambda it : Mdistance(it,knn_train,distance,k))
 return matrix.reduceByKey(lambda x,y:reduction(x,y,k)).sortByKey().map(lambda (x,y):mean(y,weight))
 
from operations.Distance import Mdistance
from dRdd import dRdd
from pyspark.sql import SparkSession
from utils.RDDUtils import reduction

def KNN_Next(rdd,d,k,n,distance="Euclidean",weight="Same"): 
 spark = SparkSession.builder.getOrCreate()
 data=dRdd(rdd,d,n)
 knn_last=(spark.sparkContext.broadcast(data.filter(lambda (x,y):x==n-d).collect()))
 matrix=data.mapPartitions(lambda it : Mdistance(it,knn_last,distance,k))
 return matrix.reduceByKey(lambda x,y:reduction(x,y,k)).map(lambda (x,y):mean(y,weight)).collect()



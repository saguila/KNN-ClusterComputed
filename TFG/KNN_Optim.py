import math
from operations.Distance import Mdistance
from dRdd import dRdd
from pyspark.sql import SparkSession
from metrics.Error import error
from metrics.Weigths import mean
from utils.RDDUtils import reduction
'''
Predicts on a known past for a time serie using KNN-Algorithm, the predict values will be the instances from init+1 to n-1 
where n is the number of time stamps on the serie to Optimize the less error cost testing  1:d*1:k values for predictions.
@Params a time serie (rdd), the dealy test range 1:d (d), the number of neighbours test range 1:k (k), init phase of the time serie (init), 
the distace type(distance), the mean weight type(weight), the error type(err),
@return a tuple with (Smallest error,the k optim value ,the d optim value)
'''
def KNN_Optim(rdd,d,k,n,distance="Euclidean",init=None,weight="Same",err="MAE"):
	spark = SparkSession.builder.getOrCreate()
	if(init == None):
		init=(n*2)/3
	optimData=False
	for i in range(1,d+1):
		data=dRdd(rdd,i,n)
		knn_train=(spark.sparkContext.broadcast(data.filter(lambda (x,y):x>init-i-1 and x!=n-i).collect()))
		matrix=data.mapPartitions(lambda it : Mdistance(it,knn_train,distance,k))
		groupMatrix=matrix.reduceByKey(lambda x,y:reduction(x,y,k))
		count=n-init
		for j in range (1,k+1):
			result=groupMatrix.map(lambda (x,y):error(mean(y[:j],weight),x[1],err)).sum()
			if(err=="MAE" or err=="ME"):
				check=(result/count,j,i)
			elif (err=="MAPE" or err== "MPE"):
				check=(result*(100/count),j,i)
			else:
				check=(math.sqrt(result/count),j,i)	
			
			if(optimData==False or optimData[0]>check[0]):
				optimData=check
		
		groupMatrix.unpersist()
	return optimData




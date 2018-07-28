import sys
from pyspark.sql import SparkSession
from knn.utils import deleteHeader 

if __name__ == '__main__':
	if len(sys.argv) == 5 :
		fileLocation = sys.argv[1]
		numPartitions = int(sys.argv[2])
		d = int(sys.argv[3])
		k = int(sys.argv[4])
		spark=SparkSession.builder.appName(sys.argv[0]).getOrCreate()
		Xpredict = spark.sparkContext.textFile(fileLocation,numPartitions).mapPartitionsWithIndex(deleteHeader).map(lambda x : float(x.split(',')[1])).cache()
		n = Xpredict.count()
		optim=knn.KNN_Optim(Xpredict,d,k,n)
		train=knn.KNN_Past(Xpredict,optim[2],optim[1],n)
		n=train.count()
		prediction=knn.KNN_Next(train,optim[2],optim[1],n)
		print(prediction)
		spark.stop()
	else:
		print("usagge <fileLocation><numPartitions><d><k>")
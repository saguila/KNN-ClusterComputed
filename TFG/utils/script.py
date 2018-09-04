Xpredict = spark.sparkContext.textFile("hdfs:///loudacre/kb/Weather.csv",16).mapPartitionsWithIndex(deleteHeader).map(lambda x : float(x.split(',')[1])).cache()
d = 3
k = 3
n = Xpredict.count()
optim=KNN_Optim(Xpredict,d,k,n)
train=KNN_Past(Xpredict,optim[2],optim[1],n)
n=train.count()
prediction=KNN_Next(train,optim[2],optim[1],n)
prediction

def KNN_Past(rdd,d,k,n,init=100,distance="Euclidean"): 
 rdd=rdd.zipWithIndex().map(lambda (x,y):(y,x))
 data=dRdd(rdd,d,n).cache()
 #Obtengo la matriz de distancias
 matrix=distanceMatrix(data,distance).cache()
 data.unpersist()
 matrix=matrix.map(lambda (x,y):(x[1],y)).groupByKey().map(lambda (x,y):(x,sorted(list(y))))
 #Ordeno los datos de la matriz de distancias correspondientes a numero n
 return matrix.filter(lambda (x,y):x[0]<n-init).sortByKey().map(lambda (x,y):(mean(y[:k],weigth)))
 

Xpredict = spark.sparkContext.textFile("hdfs:///loudacre/kb/Weather.csv").mapPartitionsWithIndex(deleteHeader).cache()
d=5
k=3
n=Xpredict.count()-d  
#Puedes pasar por parametro el tipo d distancia que quieras Manhattan,Euclidean y Canberra. Por defecto sera Euclidean
KNN_Past(Xpredict,d,k,n)
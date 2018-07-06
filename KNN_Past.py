def KNN_Past(rdd,d,k,init,distance="Euclidean"): 
 rdd=rdd.zipWithIndex().map(lambda (x,y):(y,x))
 data=dRdd(rdd, d)
 n=data.count()-1
 predictions=list()
 #Obtengo la matriz de distancias y Ordeno los datos de la matriz de distancias 
 matrix=distanceMatrix(data,distance).sortBy(lambda (x,y):y[0])
 data.unpersist()
 for i in range(2,n-init+1):
 #Obtengo los k elementos y les paso la funcion de reduccion
  sorted=getData(matrix,i).zipWithIndex().map(lambda (x,y):(y,x[1][1])).filter(lambda (x,y):x<k).map(lambda (x,y):y).cache()
  predictions.append(sorted.reduce(add)/k)
  sorted.unpersist()
 return predictions
 

Xpredict = spark.sparkContext.textFile("hdfs:///loudacre/kb/Weather.csv").mapPartitionsWithIndex(deleteHeader).cache()
d=3
k=3  
#Puedes pasar por parametro el tipo d distancia que quieras Manhattan,Euclidean y Canberra. Por defecto sera Euclidean
KNN_Next(Xpredict,d,k,40)
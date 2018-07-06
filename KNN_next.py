 #saco los puntos de data obtenidos en el take de sorted +1 Ej: n=17 en el take aparece el par (17,293) pues saco en 294
 #Hago la media artimetica de los mismos y obtengo la prediccion
def KNN_Next(rdd,d,k,distance="Euclidean"):
 rdd=rdd.zipWithIndex().map(lambda (x,y):(y,x))
 data=dRdd(rdd, d)
 n=data.count()-1
 #Obtengo la matriz de distancias
 matrix=distanceMatrix(data,distance)
 data.unpersist()
 #Ordeno los datos de la matriz de distancias correspondientes a numero n
 sorted=getData(matrix,n).sortBy(lambda (x,y):y[0]).zipWithIndex().map(lambda (x,y):(y,x[1][1])).filter(lambda (x,y):x<k).map(lambda (x,y):y).cache()
 matrix.unpersist()
 return sorted.reduce(add)/k
 
 
Xpredict = spark.sparkContext.textFile("hdfs:///loudacre/kb/Weather.csv").mapPartitionsWithIndex(deleteHeader).cache()
d=3
k=3  
#Puedes pasar por parametro el tipo d distancia que quieras Manhattan,Euclidean y Canberra. Por defecto sera Euclidean
KNN_Next(Xpredict,d,k)
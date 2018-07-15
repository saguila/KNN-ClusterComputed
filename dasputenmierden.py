 #saco los puntos de data obtenidos en el take de sorted +1 Ej: n=17 en el take aparece el par (17,293) pues saco en 294
 #Hago la media artimetica de los mismos y obtengo la prediccion
def KNN_Next(rdd,d,k,n,distance="Euclidean"):
 rdd=rdd.zipWithIndex().map(lambda (x,y):(y,x))
 data=dRdd(rdd,d,n)
 rdd=rdd.unpersist()
 data=data.cache()
 knn_last=(spark.sparkContext.broadcast(data.filter(lambda (x,y):x==n).collect()))
 #Obtengo la matriz de distancias
 return data.mapPartitions(lambda it : distance(it,knn_last,n))
 #matrix=distanceMatrix(data,distance,n)
 #data=data.unpersist()
 #matrix=matrix.filter(lambda (x,y):x==n).map(lambda (x,y):(y)).map(lambda (x):(sorted(list(x))[:k])).cache()
 #Ordeno los datos de la matriz de distancias correspondientes a numero n
 return matrix.map(lambda (x):(mean(x)))

def distance(it,knn_last,n):
 
 #fila=it.next()
 data=list()
 for fila in it:
 	data.append(euclidean_dist(fila[1][0],knn_last.value[1][0]),fila[1][1])
 #while (True):
  #try:
   #data.append(euclidean_dist(fila[1][0],knn_last.value[1][0]),fila[1][1]) 
   #fila.next()
  #except StopIteration:
   #break
 return iter(data)
 
def mean(l):
 sum = 0;
 for index in range(len(l)):
  sum += l[index][1]
 return sum / len(l)
 
 #Xpredict = spark.sparkContext.textFile("hdfs:///loudacre/kb/bigBT.csv").mapPartitionsWithIndex(deleteHeader).cache()
 
Xpredict = spark.sparkContext.textFile("hdfs:///loudacre/kb/Weather.csv").mapPartitionsWithIndex(deleteHeader).cache()
d=3
k=5
n=Xpredict.count()-d
#Puedes pasar por parametro el tipo d distancia que quieras Manhattan,Euclidean y Canberra. Por defecto sera Euclidean
KNN_Next(Xpredict,d,k,n).saveAsTextFile("hdfs:///loudacre/kb/testz")
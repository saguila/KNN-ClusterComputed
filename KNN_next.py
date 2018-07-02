 #saco los puntos de data obtenidos en el take de sorted +1 Ej: n=17 en el take aparece el par (17,293) pues saco en 294
 #Hago la media artimetica de los mismos y obtengo la prediccion
def KNN_Next(rdd,d,k,distance="Euclidean"):
 rdd=rdd.zipWithIndex().map(lambda (x,y):(y,x))
 data=dRdd(rdd, d)
 n=data.count()-1
 #Obtengo la matriz de distancias
 matrix=distanceMatrix(data,distance)
 data.unpersist()
 matrix.cache()
 #Ordeno los datos de la matriz de distancias correspondientes a numero n
 sorted=getData(matrix,n).sortBy(lambda (x,y):y)
 matrix.unpersist()
 sorted.cache()
 #Obtengo los k indices correspondientes del RDD para la prediccion
 knn=sorted.map(lambda (x,y):x).zipWithUniqueId().map(lambda (x,y):(y,adjust(n,list(x),d))).filter(lambda(x,y):x<k).map(lambda (x,y):y).take(k)
 sorted.unpersist()
 #distance=sorted.map(lambda (x,y):y).zipWithIndex().filter(lambda(x,y):x<k).map(lambda (x,y):y).collect()
 selected=rdd.filter(lambda (x,y):selectKnnValues(x,knn)).map(lambda (x,y):y).collect()
 #Falta la funcion de peso y estaria completo!!!!!!!!
 #Devuelve la media aritmetica de los valores de K seleccionados
 if k>1:
  return groupLineData(selected)
 else:
  return selected

 
#Coje los elementos KNN elegidos por ID del RDD recibido
def selectKnnValues(x,knn):
  if x in knn:
   return True
  else:
   return False


#Elije los D siguientes a los K elegidos
def adjust(n,x,d):
 if x[0]==n:
  return x[1]+d
 else:
  return x[0]+d


  
Xpredict = spark.sparkContext.textFile("hdfs:///loudacre/kb/sunspot.month.csv").mapPartitionsWithIndex(deleteHeader).cache()
d=5
k=3  
#Puedes pasar por parametro el tipo d distancia que quieras Manhattan,Euclidean y Canberra. Por defecto sera Euclidean
KNN_Next(Xpredict,d,k)
 #saco los puntos de data obtenidos en el take de sorted +1 Ej: n=17 en el take aparece el par (17,293) pues saco en 294
 #Hago la media artimetica de los mismos y obtengo la prediccion
def KNN_Next(rdd,d,k):
 data=KNN_Elements(rdd, d)
 zipped=rdd.zipWithIndex().map(lambda (x,y):(y,x))
 n=data.count()-1
 #Obtengo la matriz de distancias
 matrix=distanceMatrix(data)
 #Ordeno los datos de la matriz de distancias correspondientes a numero n
 sorted=getData(matrix,n).sortBy(lambda (x,y):y)
 #Obtengo los k indices correspondientes del RDD para la prediccion
 knn=sorted.map(lambda (x,y):x).zipWithIndex().map(lambda (x,y):(y,adjust(n,list(x),d))).filter(lambda(x,y):x<k).map(lambda (x,y):y).collect()
 distance=sorted.map(lambda (x,y):y).zipWithIndex().filter(lambda(x,y):x<k).map(lambda (x,y):y).collect()
 selected=zipped.filter(lambda (x,y):selectKnnValues(x,knn)).map(lambda (x,y):y).collect()
 #Falta la funcion de peso y estaria completo
 #Devuelve la media aritmetica de los valores de K seleccionados
 return float(sum(selected)) / max(len(selected), 1)


 
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


  
Xpredict = spark.sparkContext.textFile("hdfs:///loudacre/kb/Weather.csv").mapPartitionsWithIndex(deleteHeader).map(lambda x:np.array(x.split(","))).map(lambda x: np.float(x[1]))
d=5
k=3  
KNN_Next(Xpredict,d,k)
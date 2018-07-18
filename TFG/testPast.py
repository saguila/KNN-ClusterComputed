def KNN_Past(rdd,d,k,n,distance="Euclidean",init=400,weigth="Same"):
 rdd=rdd.zipWithIndex().map(lambda (x,y):(y,x))
 data=dRdd(rdd,d,n).cache()
 knn_train=(spark.sparkContext.broadcast(data.filter(lambda (x,y):x>init-d-1 and x!=n).collect()))
 #Obtengo la matriz de distancias
 matrix=data.mapPartitions(lambda it : Mdistance(it,knn_train,init))
 return matrix.groupByKey().map(lambda (x,y):(x,list(y))).map(lambda(x,y):(x,sorted(y))).sortByKey().map(lambda (x,y):mean(y[:k],weigth))
 
 
 
def Mdistance(it,knn_train,n):
 data1=[]
 for fila in it:
  for i in range(0,len(knn_train.value)):
   if(knn_train.value[i][0]>fila[0]):
    data1.append((knn_train.value[i][0],[euclidean_dist(fila[1][0],knn_train.value[i][1][0]),fila[1][1]]))
 return iter(data1)
def KNN_Optim(rdd,d,k,n,distance="Euclidean",init=400,weigth="Same",err="MAE"):
 rdd=rdd.zipWithIndex().map(lambda (x,y):(y,x))
 optimData=False
 for i in range(1,d+1):
  data=dRdd(rdd,i,n-i)
  knn_train=(spark.sparkContext.broadcast(data.filter(lambda (x,y):x>init-i-1 and x!=n-i).collect()))
  #Obtengo la matriz de distancias
  matrix=data.mapPartitions(lambda it : Mdistance(it,knn_train))
  groupMatrix=matrix.groupByKey().map(lambda (x,y):(x,list(y))).map(lambda(x,y):(x,sorted(y))).cache()
  count=n-init
  for j in range (1,k+1):
   result=groupMatrix.map(lambda (x,y):error(mean(y[:j],weigth),x[1],err)).sum()
   if(err=="MAE" or err=="ME"):
    check=(result/count,j,i)
   elif (err=="MAPE" or err== "MPE"):
    check=(result*(100/count),j,i)
   else:
    check=(round(np.sqrt(result/count),5),j,i)
   if(optimData==False or optimData[0]>=check[0]):
    optimData=check
 return optimData


def Mdistance(it,knn_train):
 data1=[]
 for fila in it:
  for i in range(0,len(knn_train.value)):
   if(knn_train.value[i][0]>fila[0]):
    data1.append(((knn_train.value[i][0],knn_train.value[i][1][1]),[euclidean_dist(fila[1][0],knn_train.value[i][1][0]),fila[1][1]]))
 return iter(data1)

 
Xpredict = spark.sparkContext.textFile("hdfs:///loudacre/kb/Weather.csv",4).mapPartitionsWithIndex(deleteHeader).cache()
d=3
k=3
n=Xpredict.count()
#Puedes pasar por parametro el tipo d distancia que quieras Manhattan,Euclidean y Canberra. Por defecto sera Euclidean
KNN_Optim(Xpredict,d,k,n)
 
 
 
 
#ESTO ERA UN EXPERIMENTO VA MAS RAPIDO PERO AL AGRUPARLO TOMA EL ARRAY COMO UN ELEMENTO INDIVIDUAL NO SE PUEDE ACCEDER 
#SI ENCONTRAMOS SOLUCION IRIA UNOS SEGUNDOS MAS RAPIDO
def Mdistance(it,knn_train):
 data=[]
 for i in range (0,len(knn_train.value)):
  data.append([])
  data[i].append((knn_train.value[i][0],knn_train.value[i][1][1]))
 for fila in it:
  for i in range(0,len(knn_train.value)):
    if(knn_train.value[i][0]>fila[0]):
     data[i].append([euclidean_dist(fila[1][0],knn_train.value[i][1][0]),fila[1][1]])
 return iter(data)
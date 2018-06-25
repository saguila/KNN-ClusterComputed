 #saco los puntos de data obtenidos en el take de sorted +1 Ej: n=17 en el take aparece el par (17,293) pues saco en 294
 #Hago la media artimetica de los mismos y obtengo la prediccion
def KNN_Next(rdd,d,k):
 data=dRddv2(rdd, d)
 zipped=rdd.zipWithIndex().map(lambda (x,y):(y,x))
 n=data.count()-1
 matrix=distanceMatrix(data)
 sorted=getData(matrix,n).sortBy(lambda (x,y):y)
 knn=sorted.map(lambda (x,y):x).map(lambda x:adjust(n,list(x))).take(k)
 selected=zipped.filter(lambda (x,y):selectKnnValues(x,knn)).map(lambda (x,y):y).take(k)
 return float(sum(selected)) / max(len(selected), 1)

 
def selectKnnValues(x,knn):
  if x in knn:
   return True
  else:
   return False
 
 
def adjust(n,x):
 if x[0]!= n :
  return x[0]+1
 else:
  return x[1]+1

d=3
k=3  
KNN_Next(Xpredict,d,k)
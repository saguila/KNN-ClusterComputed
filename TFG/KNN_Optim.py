def KNN_Optim(rdd,d,k,n,init=100,distance="Euclidean",Error="): 
 rdd=rdd.zipWithIndex().map(lambda (x,y):(y,x))
 optimos=[]
 
 for i in range (1,d+1):
  actualN=n-i
  data=dRdd(rdd,i,actualN)
  listd=[]
  #Obtengo la matriz de distancias
  matrix=distanceMatrix(data,distance)
  data.unpersist()
  matrix=matrix.filter(lambda (x,y):x[0]<actualN-init).map(lambda (x,y):(x,sorted(list(y)))).cache()
 #Ordeno los datos de la matriz de distancias correspondientes a numero n
  for j in range(1,k):
   listd.append(error(matrix.map(lambda (x,y):[abs(mean(y[:j])-x[1])).sum())/actualN),j])
  best=sorted(listd)[0]
  best.append(i)
  optim.append(best) 
  matrix.unpersist()
 return sorted(optim)[0]
  

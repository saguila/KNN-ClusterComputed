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
  
 
 
def distanceMatrix(rdd,distance):
 data=rdd.cartesian(rdd).filter(lambda (x,y):x[0]<y[0])
 data=data.map(lambda (x,y):((x[0],y[0]),[x[1:],y[1:]]))
 measure = {
	"Manhattan":data.map(lambda (x,y):((x[1],y[1][0][1]),[manhattan_dist(y[0][0][0],y[1][0][0]),y[0][0][1]])).groupByKey(),
	"Euclidean":data.map(lambda (x,y):((x[1],y[1][0][1]),[euclidea_dist(y[0][0][0],y[1][0][0]),y[0][0][1]])).groupByKey(),
	"Canberra":data.map(lambda (x,y):((x[1],y[1][0][1]),[canberra_dist(y[0][0][0],y[1][0][0]),y[0][0][1]])).groupByKey()
		 }
 return measure.get(distance,"Wrong distance")
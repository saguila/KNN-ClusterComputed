def groupMapping(rdd,index,d):
    return rdd.map(lambda (x,y) : (x - index,y))


#Crea el rdd con las combinaciones de d
def dRdd(rddInput, d):
 rddList = []
 rdd=rddInput.zipWithIndex().map(lambda (x,y):(x[0],[y,x[1]]))
 rddJoin=rddInput.map(lambda(x,y):(x-d,float(y.split(",")[1])))
 rddList.append(rdd)
 for index in range(1,d):
  rddList.append(groupMapping(rdd,index,d))
 rdd=spark.sparkContext.union(rddList).groupByKey().filter(lambda (x,y) : len(y) == d).mapValues(lambda x: groupLineData(sorted(list(x))))
 n=rdd.count()
 pred=rdd.filter(lambda(x,y):x==n-1).map(lambda(x,y):(x,(y,'x')))
 return rdd.join(rddJoin).union(pred).cache()
	

#Une la D segun la columna correspondiente de los elementos y hace la media de la misma
def groupLineData(string):
 aux=list()
 out=list()
 for i in range(0,len(string)):
  aux.append(string[i][1].split(","))
 for i in range(0,len(aux)):
  for j in range (1,len(aux[0])):
   out.append(float(aux[i][j]))
 return out
		

		
## Esto son pruebas de funcionamiento ##
rdd = spark.sparkContext.parallelize(['11,12,13','21,22,23','31,32,33','41,42,43','51,52,53','61,62,63','71,72,73','81,82,83','91,92,93','01,02,03']).zipWithIndex().map(lambda (x,y):(y,x)).cache()

dRdd(rdd,3).collect()

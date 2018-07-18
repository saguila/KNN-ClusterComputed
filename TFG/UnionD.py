def groupMapping(rdd,index,d):
    return rdd.map(lambda (x,y) : (x - index,y))


#Crea el rdd con las combinaciones de d

def dRdd(rddInput, d,n):
 rddList = []
 rdd=rddInput.zipWithIndex().map(lambda (x,y):(x[0],[y,x[1]]))
 rddJoin=rddInput.map(lambda(x,y):(x-d,float(y.split(",")[1])))
 rddList.append(rdd)
 for index in range(1,d):
  rddList.append(groupMapping(rdd,index,d))
 rdd=spark.sparkContext.union(rddList).groupByKey().filter(lambda (x,y) : len(y) == d).mapValues(lambda x: groupLineData(sorted(list(x))))
 pred=rdd.filter(lambda(x,y):x==n).map(lambda(x,y):(x,(y,'x')))
 return rdd.join(rddJoin).union(pred)
	

#Une la D segun la columna correspondiente de los elementos

def groupLineData(string):
 aux=list()
 out=list()
 for i in range(0,len(string)):
  aux.append(string[i][1].split(","))
 for i in range(0,len(aux)):
  for j in range (1,len(aux[0])):
   out.append(float(aux[i][j]))
 return out
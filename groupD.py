#Esta funcion se pone porque python si se modifica la variable sigue apuntando a la misma direccion de memoria y hace appends a la lista del mismo rdd continuamente
def groupMapping(rdd,index,d):
	return rdd.map(lambda (x,y) : (x - index,y))

#Crea el rdd con las combinaciones de d
def dRddv2(rddInput, d):
	rddList = []
	rddInput = rddInput.zipWithIndex().map(lambda (x,y) : (y,x))
	rddList.append(rddInput)
	for index in range(1,d):
		rddList.append(groupMapping(rddInput,index,d))
	return spark.sparkContext.union(rddList).groupByKey().filter(lambda (x,y) : len(y) == d).mapValues(list)


#Esta funcion se pone porque python si se modifica la variable sigue apuntando a la misma direccion de memoria y hace appends a la lista del mismo rdd continuamente
def groupMapping(rdd,index,d):
    return rdd.map(lambda (x,y) : (x - index,y))


#Crea el rdd con las combinaciones de d
def dRdd(rddInput, d):
    rddList = []
    rddInput = rddInput.zipWithIndex().map(lambda (x,y) : (y,x))
    rddList.append(rddInput)
    for index in range(1,d):
        rddList.append(groupMapping(rddInput,index,d))
    return spark.sparkContext.union(rddList).groupByKey().filter(lambda (x,y) : len(y) == d).mapValues(lambda x: groupLineData(list(x)))

	

#Une la D segun la columna correspondiente de los elementos y hace la media de la misma
def groupLineData(string):
 aux=list()
 out=list()
 for i in range(0,len(string)):
  aux.append(string[i].split(","))
 for i in range(0,len(aux[0])):
  aux2=list()
  for j in range (0,len(aux)):
   aux2.append(float(aux[j][i]))
  out.append(np.mean(aux2))
 return out
		

		
## Esto son pruebas de funcionamiento ##
rdd = spark.sparkContext.parallelize(['11,12,13','21,22,23','31,32,33','41,42,43','51,52,53','61,62,63','71,72,73','81,82,83','91,92,93','01,02,03']).cache()

#.zipWithIndex().map(lambda (x,y) : (y,x))

#emptyRdd = spark.sparkContext.emptyRDD().union(rddList)

dRdd(rdd,3).collect()

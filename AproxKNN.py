import numpy as np
import math


#Esta funcion se pone porque python si se modifica la variable sigue apuntando a la misma direccion de memoria y hace appends a la lista del mismo rdd continuamente
def groupMapping(rdd,index,d):
    return rdd.map(lambda (x,y) : (x - index,y))

""" TODO:Intetar agrupar mapValues segundo mapValues en el primero map Values,
    El segundo mapValues lo que hace es hacer la media para esa clase o ese prediccion 
    ,El ultimo map values lo que hace es pasar el array a un kv con k array de las clases
    y v el valor predecido. Arreglar porque esta funcion esta mal identada """

#Crea el rdd con las combinaciones de d
def dRdd(rddInput, d,combFunc = lambda x:np.mean(x,0)):
    rddList = []
    rddInput = rddInput.zipWithIndex().map(lambda (x,y) : (y,x))
    rddList.append(rddInput)
    for index in range(1,d):
        rddList.append(groupMapping(rddInput,index,d))
    return spark.sparkContext.union(rddList).groupByKey().filter(lambda (x,y) : len(y) == d).mapValues(list).mapValues(lambda x : combFunc(x)).mapValues(lambda x : (x[0:len(x) - 1],x[len(x) - 1]))

#knn_train es la funcion que ayuda a elegir los valores correctos de k y d
#XtrainYtrain es un rdd que incluye las Y en formato numpy array en la ultima posicion del array,Xpredict es un rdd con las X en formato numpyArray
#Devuelve el error metrico para esa k y esa d
def knn_train(k,d,XtrainYtrain,Xpredict):
    #apartir de este indice estan todos los datos que tenemos que predecir
    predictSize = Xpredict.count()
    rddToProcess = XtrainYtrain.union(Xpredict)
    totalSize = rddToProcess.count()
    trainSize = totalSize - predictSize
    predictGroupIndex = trainSize - d
    #Recogemos lo agrupado por la func
    DataGrouped = dRdd(rddToProcess,d)
    #TODO: Ahorrarse el separar el rdd en los de training y predict con los generatePairRdd se puede hacer si juntas el rdd Grouped 2 veces contra este o cambiarlo por un producto cartesiano todo y un filter
    #Recuperamos de todo el conjunto agrupado los ejemplos de entrenamiento
    YtrainGrouped = DataGrouped.filter(lambda (x,y) : x <= predictGroupIndex)
    #Recuperaremos los ejemplos para prediccion,los primeros es posible que tengan una mezcla de puntos que pertenecen a ejemplos de entrenamiento
    XpredictGrouped = DataGrouped.filter(lambda (x,y) : x > predictGroupIndex)
    #Calcular la distancia la salida es un rdd con el tipo (XaPredecir,(La distancia,(YpredecidaParaEsePunto,Ycorrecta)) Da igual saber el punto lo unico que nos interesa es la Y predecida
    rddWithDistances = generatePairRdd(predictGroupIndex,totalSize - d).join(YtrainGrouped).map(lambda (x,y) : y).join(XpredictGrouped).map(lambda(x,y):(x,(euclidea_dist(y[0][0],y[1][0]),(y[0][1],y[1][1]))))
    #Los siguientes pasos seria ordenar de menor a mayot r por distancias y sacar los k elementos que nos interesen sacar la media para ver 
    rddSortedByPositionDistance = rddWithDistances.sortBy(lambda (x,y) : (x,y[0]))
    
def euclidea_dist(x,y):
    dist = 0
    for i in range(0, len(x) if len(x) > len(y) else len(y)):
        dist += (x[i] - y[i])**2
    return math.sqrt(dist)

	
#Genera el rdd para hacer las ayudar a hacer las uniones de los ejemplos de entrenamiento
def generatePairRdd(indexPartitioner, totalItems):
    x = []
    for i in range(0, indexPartitioner + 1):
        for j in range(indexPartitioner + 1, totalItems + 1):
            x.append((i, j))
    for i in range(indexPartitioner + 1,totalItems + 1):
        for j in range(i, totalItems + 1):
            if i != j:
                x.append((i, j))
    return spark.sparkContext.parallelize(x)

#TODO: Esta funcion devolvera el rrd con los valores predecidos
def knn_estimator(k,d,Knowledge,Xpredict):
    #....#

#Funcion para quitar la cabecera de un csv ayudandose de una funcion map
def deleteHeader(idx, iter):
    output=[]
    for sublist in iter:
        output.append(sublist)
    if idx>0:
        return(output)
    else:
        return(output[1:])



## PRUEBA DE EJECUCION DEL ALGORITMO ##

#Para esta primera aproximación sin buscar el k y el d optimo usamos esta configuracion

k = 3
d = 2

#Leo los ejemlos de entrenamiento y los tranformo para que este en el tipo de dato soportado por mi algoritmo
XtrainYtrain = spark.sparkContext.textFile("file:///Users/saguila/bitcoinTrain.csv").mapPartitionsWithIndex(deleteHeader).map(lambda line : np.array(line.split(',')).astype(np.float))
#XtrainYtrain = spark.sparkContext.textFile("file:///C:/bitcoinTrain.csv").mapPartitionsWithIndex(deleteHeader).map(lambda line : np.array(line.split(',')).astype(np.float))
#Leo los ejemplos sobre los que hago la prediccion
Xpredict = spark.sparkContext.textFile("file:///Users/saguila/bitcoinPredict.csv").mapPartitionsWithIndex(deleteHeader).map(lambda line : np.array(line.split(',')).astype(np.float))
#Xpredict = spark.sparkContext.textFile("file:///C:/bitcoinPredict.csv").mapPartitionsWithIndex(deleteHeader).map(lambda line : np.array(line.split(',')).astype(np.float))
#Hay que comentar con el profesor una logica para ir probando d y k para buscar el optimo de manera correcta,teneniendo en cuenta sesgo-varianza

#Si la varianza es muy grande es porque nos ajustamos mucho a la funcion por lo tanto estamos usando un k muy alta.
#Con un d grande lo que se hace es generalizar el modelo
#Conclusiones : k > sobre ajuste del modelo d > generalizacion del modelo , hay que buscar un equilibrio

errorMetrico = kdd_train(k,d,XtrainYtrain,Xpredict)

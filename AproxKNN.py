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
    predictGroupIndex = XtrainYtrain.count() - d
    rddToProcess = XtrainYtrain.union(Xpredict);
    #Recogemos lo agrupado por la func
    DataGrouped = dRdd(rddToProcess,d)
    #YtrainGrouped = TODO: coger los que tienen un indice <= predictGroupIndex
    #No hacer nada con 
    #XpredictGrouped = coger los que tienen un indice > predictGroupIndex
    """TODO: 
        (1) Con los XpredictGrouped hacemos join con XtrainGrouped para calcular las distancias,
        (2) Una vez que tenemos las distancias ordenamos el rdd con las distancias de menor a mayor
        (3) sacamos las k primeras distancias(las menores) y en esas filas estaran las Y hacemos una media y sacamos el valor
    """

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

#Leo los ejemplos sobre los que hago la prediccion
Xpredict = spark.sparkContext.textFile("file:///Users/saguila/bitcoinPredict.csv").mapPartitionsWithIndex(deleteHeader).map(lambda line : np.array(line.split(',')).astype(np.float))

#Hay que comentar con el profesor una logica para ir probando d y k para buscar el optimo de manera correcta,teneniendo en cuenta sesgo-varianza

#Si la varianza es muy grande es porque nos ajustamos mucho a la funcion por lo tanto estamos usando un k muy alta.
#Con un d grande lo que se hace es generalizar el modelo
#Conclusiones : k > sobre ajuste del modelo d > generalizacion del modelo , hay que buscar un equilibrio

errorMetrico = kdd_train(k,d,XtrainYtrain,Xpredict)

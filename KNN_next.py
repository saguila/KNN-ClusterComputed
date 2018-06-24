def KNN_Next(rdd,d,k):
 data=dRddv2(rdd, d)
 n=dRddv2.count()-1
 matrix=distanceMatrix(data)
 sorted=getData(matrix,n).sortBy(lambda (x,y):y)
 #sorted.take(k)
 #saco los puntos de data obtenidos en el take de sorted +1 Ej: n=17 en el take aparece el par (17,293) pues saco en 294
 #Hago la media artimetica de los mismos y obtengo la prediccion
import math
 
#Devuelve la distancia euclidea entre 2 vectores
def euclidean_dist(x,y):
    dist = 0
    for i in range(0, len(x) if len(x) > len(y) else len(y)):
		for j in range(0,len(x[i][1])):
			dist += (x[i][1][j] - y[i][1][j])**2
    return math.sqrt(dist)


	
def manhattan_dist(x,y):
	dist = 0
	for i in range(0, len(x) if len(x) > len(y) else len(y)):
		for j in range(0,len(x[i][1])):
			dist += abs(x[i][1][j] - y[i][1][j])
	return dist



def canberra_dist(x,y):
	dist = 0
	try:
		for i in range(0, len(x) if len(x) > len(y) else len(y)):
			for j in range(0,len(x[i][1])):
				if(x[i][1][j]!=0 and y[i][1][j]!=0):
					dist += (abs(x[i][1][j] - y[i][1][j]))/(abs(x[i][1][j])+abs(y[i][1][j]))
		return dist
	except ZeroDivisionError:
		print("error canberra")



def Mdistance(it,knn_train,distance,k):
	data1=[]
	data=[]
	for i in range(0,len(knn_train.value)):
		data1.append(((knn_train.value[i][0],knn_train.value[i][1][1][1][0]),[]))
	for fila in it:
		for i in range(0,len(knn_train.value)):
			if(knn_train.value[i][0]>fila[0]):
				dist = {
				"Euclidean":euclidean_dist(fila[1][0],knn_train.value[i][1][0]),
				"Manhattan":manhattan_dist(fila[1][0],knn_train.value[i][1][0]),
				"Canberra":canberra_dist(fila[1][0],knn_train.value[i][1][0])
				}
				data1[i][1].append([dist.get(distance,"Wrong distance"),-fila[0],fila[1][1][1][0]])
	for i in range(0,len(knn_train.value)):
		data.append((data1[i][0],sorted(data1[i][1])[:k]))
	return iter(data)
 
 